package nano

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

type rpcHandler func(string, []byte) (string, []byte)

func rpcListen(dbURI string, name string, handler rpcHandler) error {
	log.Debug("[RPC] listenning")

	conn, err := amqp.Dial(dbURI)
	if err != nil {
		return err
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	defer ch.Close()

	q, err := ch.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return err
	}

	log.Debugf("AMQP Queue created: %s\n", q.Name)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	for d := range msgs {
		go handleReq(ch, d, handler)
	}
	return errors.New("Connection Lost")
}

func handleReq(ch *amqp.Channel, d amqp.Delivery, handler rpcHandler) {
	contentType, body := handler(d.ContentType, d.Body)

	err := ch.Publish(
		"",        // exchange
		d.ReplyTo, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:   contentType,
			CorrelationId: d.CorrelationId,
			Body:          body,
		})

	if err != nil {
		log.Error("Failed to publish a message: " + err.Error())
		return
	}

	d.Ack(false)
}
