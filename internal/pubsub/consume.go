package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int
type Acktype int

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	clientChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	var durable, autoDelete, exclusive bool
	switch queueType {
	case Durable:
		durable = true
		autoDelete = false
		exclusive = false
	case Transient:
		durable = false
		autoDelete = true
		exclusive = true
	default:
		return nil, amqp.Queue{}, fmt.Errorf("unknown SimpleQueueType")
	}

	newQueue, err := clientChannel.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = clientChannel.QueueBind(newQueue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}

	return clientChannel, newQueue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	subCh, subQueue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}

	newChan, err := subCh.Consume(subQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		defer subCh.Close()
		for ch := range newChan {
			var msg T
			err := json.Unmarshal(ch.Body, &msg)
			if err != nil {
				log.Fatalf("error unmarshalling: %v", err)
				continue
			}
			ackNack := handler(msg)
			switch ackNack {
			case Ack:
				ch.Ack(false)
				log.Println("Ack occured!")
			case NackRequeue:
				ch.Nack(false, true)
				log.Println("NackRequeue occured!")
			case NackDiscard:
				ch.Nack(false, false)
				log.Println("NackDiscard occured!")
			}
		}
	}()

	return nil
}
