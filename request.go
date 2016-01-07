package nano

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/streamrail/concurrent-map"
	"strconv"
	"sync/atomic"
	"time"
)

type rpcQueue struct {
	name          string
	channels      cmap.ConcurrentMap
	amqpChannel   *amqp.Channel
	correlationId uint64
}

var li int

func (q *rpcQueue) Request(routingKey string, contentType string, body []byte) (string, []byte, error) {
	corrId := strconv.FormatUint(atomic.AddUint64(&(q.correlationId), 2), 10)
	resChan := make(chan *amqp.Delivery)

	_, exists := q.channels.Get(corrId)
	if exists {
		log.Error("Correlation id already exists")
		return "", nil, errors.New("Correlation id already exists")
	}

	q.channels.Set(corrId, resChan)

	fmt.Printf("Send request to %s\n", routingKey)

	err := q.amqpChannel.Publish(
		"",         // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:   contentType,
			CorrelationId: corrId,
			ReplyTo:       q.name,
			Body:          body,
		})

	if err != nil {
		return "", nil, err
	}

	d := <-resChan

	return d.ContentType, d.Body, nil
}

func newRPCQueue(uri string) (*rpcQueue, error) {
	fmt.Println("newRPCQueue")
	var conn *amqp.Connection
	var err error

	for try := 0; try < 10; try++ {
		log.Infof("Attempt to connect to RabbitMQ: %d", try+1)
		conn, err = amqp.Dial(uri)
		if err == nil {
			break
		}
		time.Sleep(time.Second * 5)
	}
	if err != nil {
		return nil, err
	}

	amqpChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	queue, err := amqpChannel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		amqpChannel.Close()
		conn.Close()
		return nil, err
	}

	name := queue.Name

	msgs, err := amqpChannel.Consume(
		name,  // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		amqpChannel.Close()
		conn.Close()
		return nil, err
	}

	q := rpcQueue{
		name:          name,
		channels:      cmap.New(),
		amqpChannel:   amqpChannel,
		correlationId: 0,
	}

	go func() {
		for d := range msgs {
			corrId := d.CorrelationId
			context, exists := q.channels.Get(corrId)
			if exists {
				q.channels.Remove(corrId)
				context.(chan *amqp.Delivery) <- &d
			} else {
				log.Errorf("Cannot handle RPC request (correlation id = %s)", corrId)
			}
		}
	}()

	return &q, nil
}
