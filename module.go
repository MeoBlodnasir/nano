package nano

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

type User struct {
	Id              string
	Email           string
	Activated       bool
	IsAdmin         bool
	FirstName       string
	LastName        string
	Sam             string
	WindowsPassword string
}

type handler func(Request) (*Response, error)

type reqHandler struct {
	handlers *[]handler
	pattern  string
}

type request struct {
	Method      string `json:"method"`
	Path        string `json:"path"`
	ContentType string `json:"content_type"`
	Body        []byte `json:"body"`
	User        *User  `json:"user"`
}

type Response struct {
	StatusCode  int
	ContentType string
	Body        []byte
}

type Request struct {
	Query  map[string][]string
	Body   []byte
	User   *User
	Params map[string]string
}

type Module struct {
	Log *log.Entry

	name  string
	queue *rpcQueue

	/*
	 * Map of string (http method), array of *reqHandler
	 */
	handlers map[string][]*reqHandler
}

type Error struct {
	StatusCode int    `json:"status_code"`
	Message    string `json:"error"`
}

func JSONResponse(statusCode int, body interface{}) *Response {
	res := Response{
		ContentType: "application/json",
	}

	b, err := json.Marshal(body)
	if err != nil {
		res.StatusCode = 500
		log.Error(err)
		res.Body = []byte(`{"error":"Internal Server Error"}`)
		return &res
	}

	res.StatusCode = statusCode
	res.Body = b
	return &res
}

func (e Error) Error() string {
	return e.Message
}

func (m *Module) Listen() {
	amqpURI := os.Getenv("AMQP_URI")
	if len(amqpURI) == 0 {
		amqpURI = "amqp://guest:guest@localhost:5672/"
	}

	var err error
	for try := 0; try < 10; try++ {
		err = rpcListen(amqpURI, m.name, m.handleReq)
		if err.Error() == "Connection lost" {
			try = 0
		}
		time.Sleep(time.Second * 5)
	}

	if err != nil {
		panic(err)
	}
}

func getModuuleNameFromPath(path string) string {
	u, err := url.Parse(path)
	if err != nil {
		return ""
	}

	s := strings.SplitN(u.Path, "/", 3)
	if len(s) > 1 {
		return s[1]
	}
	return ""
}

func (m *Module) JSONRequest(method string, path string, body interface{}, user *User) (*Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return m.Request(method, path, "application/json", b, user)
}

func (m *Module) Request(method string, path string, contentType string, body []byte, user *User) (*Response, error) {
	routingKey := getModuuleNameFromPath(path)

	b, err := json.Marshal(request{
		Path:        path,
		Method:      method,
		ContentType: contentType,
		Body:        body,
		User:        user,
	})

	if err != nil {
		panic(err)
	}

	contentType, body, err = m.queue.Request(routingKey, "application/json", b)

	if !isJSON(contentType) {
		return nil, errors.New("Invalid content-type")
	}

	response := Response{}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (m *Module) addHandler(method string, pattern string, handlers *[]handler) {
	h, exists := m.handlers[method]
	if !exists {
		m.handlers[method] = []*reqHandler{
			&reqHandler{
				pattern:  pattern,
				handlers: handlers,
			},
		}
	}

	m.handlers[method] = append(h, &reqHandler{
		pattern:  pattern,
		handlers: handlers,
	})
}

func (m *Module) Get(pattern string, handlers ...handler) {
	m.addHandler("GET", pattern, &handlers)
}

func (m *Module) Post(pattern string, handlers ...handler) {
	m.addHandler("POST", pattern, &handlers)
}

func (m *Module) Delete(pattern string, handlers ...handler) {
	m.addHandler("DELETE", pattern, &handlers)
}

func (m *Module) Put(pattern string, handlers ...handler) {
	m.addHandler("PUT", pattern, &handlers)
}

func isJSON(contentType string) bool {
	return strings.SplitN(contentType, ";", 2)[0] == "application/json"
}

func jsonError(statusCode int, message string) (string, []byte) {
	e := Error{
		StatusCode: 500,
		Message:    message,
	}

	r, err := json.Marshal(e)
	if err != nil {
		return "application/json", []byte(`{"status_code":500,"error":"Unable to serizalize original error"}`)
	}

	return "application/json", r
}

func patternMatch(pattern, path string) (map[string]string, bool) {
	if path == "/" {
		if pattern == "/" {
			return nil, true
		}
		return nil, false
	}

	p := strings.Split(pattern, "/")
	r := strings.Split(path, "/")

	if len(p) != len(r) {
		return nil, false
	}

	m := make(map[string]string)

	for i := 0; i < len(p); i++ {
		if strings.HasPrefix(p[i], ":") {
			k := strings.TrimPrefix(p[i], ":")
			m[k] = r[i]
		} else if p[i] != r[i] {
			return nil, false
		}
	}

	return m, true
}

func (m *Module) handleReq(contentType string, body []byte) (string, []byte) {
	if !isJSON(contentType) {
		return jsonError(500, "invalid rpc message content-type")
	}

	req := request{}
	err := json.Unmarshal(body, &req)
	if err != nil {
		panic(err)
	}

	u, err := url.Parse(req.Path)
	if err != nil {
		panic(err)
	}

	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		panic(err)
	}

	r := Request{
		Query: query,
		Body:  req.Body,
		User:  req.User,
	}

	handlers, exists := m.handlers[req.Method]
	if exists {
		for i := 0; i < len(handlers); i++ {
			params, ok := patternMatch(handlers[i].pattern, u.Path)
			if ok {
				r.Params = params
				var err error
				var response *Response

				for _, handler := range *handlers[i].handlers {
					response, err = handler(r)
					if err != nil {
						return jsonError(500, err.Error())
					}
					if response != nil {
						b, err := json.Marshal(response)
						if err != nil {
							return jsonError(500, err.Error())
						}

						return "application/json", b
					}
				}

				return jsonError(500, err.Error())
			}
		}
	}

	return jsonError(404, "Not Found")
}

func newModule(name string) (*Module, error) {
	amqpURI := os.Getenv("AMQP_URI")
	if len(amqpURI) == 0 {
		amqpURI = "amqp://guest:guest@localhost:5672/"
	}

	logger := log.WithFields(log.Fields{
		"module": name,
	})

	queue, err := newRPCQueue(amqpURI)
	if err != nil {
		return nil, err
	}

	return &Module{
		Log:      logger,
		name:     name,
		queue:    queue,
		handlers: make(map[string][]*reqHandler),
	}, nil
}
