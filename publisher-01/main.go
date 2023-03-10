package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

//
//func failOnError(err error, msg string) {
//	if err != nil {
//		log.Panicf("%s: %s", msg, err)
//	}
//}
//
//func Publish(ch *amqp.Channel, q amqp.Queue, body []byte) (err error) {
//
//	err = ch.PublishWithContext(context.TODO(), "", q.Name, false, false, amqp.Publishing{
//		ContentType: "application/json",
//		Body:        body,
//	})
//
//	return
//}
//
//func Consume(ch *amqp.Channel, q amqp.Queue) (err error) {
//	msgs, err := ch.Consume(
//		q.Name, // queue
//		"",     // consumer
//		true,   // auto-ack
//		false,  // exclusive
//		false,  // no-local
//		false,  // no-wait
//		nil,    // args
//	)
//	failOnError(err, "Failed to register a consumer")
//
//	for d := range msgs {
//		log.Printf("Received a message: %s", d.Body)
//	}
//
//	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
//	return
//}
//
//func main() {
//	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
//	failOnError(err, "Failed to connect to RabbitMQ")
//
//	ch, err := conn.Channel()
//	failOnError(err, "Failed to open a channel")
//
//	q, err := ch.QueueDeclare(
//		"topic-01", // name
//		false,      // durable
//		false,      // delete when unused
//		false,      // exclusive
//		false,      // no-wait
//		nil,        // arguments
//	)
//
//	failOnError(err, "Failed to declare a queue")
//
//	counter := 1
//
//	// cron job
//	//for {
//	//	msg := fmt.Sprintf(`{"Message" : "Hello World", "Number" : %v }`, counter)
//	//	fmt.Println("Send :", msg)
//	//	err = Publish(ch, q, []byte(msg))
//	//	failOnError(err, "Failed publish message")
//	//
//	//	time.Sleep(time.Second * 1)
//	//}
//
//	// goroutine
//	var mu sync.Mutex
//	var forever = make(chan struct{})
//
//	for i := 1; i <= 1000; i++ {
//		go func() {
//			mu.Lock()
//			msg := fmt.Sprintf(`{"Message" : "Hello World", "Number" : %v }`, counter)
//			fmt.Println("Send :", msg)
//			err = Publish(ch, q, []byte(msg))
//			failOnError(err, "Failed publish message")
//			counter++
//			mu.Unlock()
//		}()
//	}
//
//	defer ch.Close()
//	defer conn.Close()
//
//	<-forever
//
//}

type RabbitmqUtils struct {
	Url  string
	conn *amqp.Connection
	ch   *amqp.Channel
}

func (that *RabbitmqUtils) Connect() (err error) {
	that.Url = "amqp://guest:guest@localhost:5672/"
	if that.Url == "" {
	}
	that.conn, err = amqp.Dial(that.Url)
	if err != nil {
		return
	}

	that.ch, err = that.conn.Channel()
	if err != nil {
		return
	}

	return
}

func (that *RabbitmqUtils) Close() {
	that.ch.Close()
	that.conn.Close()
}

func (that *RabbitmqUtils) DirectPublish(queueName string, message []byte) (err error) {

	err = that.ch.PublishWithContext(context.TODO(), "", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        message,
	})

	return
}

func (that *RabbitmqUtils) InitQueue(name string, callback func(message []byte)) (err error) {

	msgs, err := that.ch.Consume(
		name,  // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		return
	}

	for d := range msgs {
		callback(d.Body)
	}

	return
}

func main() {
	var err error
	mq := RabbitmqUtils{}
	err = mq.Connect()
	if err != nil {
		fmt.Println(err.Error())
	}
	i := 1
	//msg := fmt.Sprintf(`{"Message" : "Hello World", "Number" : %v }`, i)
	//err = mq.DirectPublish("topic-03", []byte(msg))
	//if err != nil {
	//	fmt.Println(err.Error())
	//}
	//
	//fmt.Println("Send:", msg)

	for {
		msg := fmt.Sprintf(`{"Message" : "Hello World", "Number" : %v }`, i)
		err = mq.DirectPublish("topic-03", []byte(msg))
		if err != nil {
			fmt.Println(err.Error())
		}

		fmt.Println("Send:", msg)
		i++

		time.Sleep(1 * time.Second)
	}

	mq.Close()
}
