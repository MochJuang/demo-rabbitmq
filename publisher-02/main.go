package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func Publish(ch *amqp.Channel, q amqp.Queue, body []byte) (err error) {

	err = ch.PublishWithContext(context.TODO(), "", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})

	return
}

func Consume(ch *amqp.Channel, q amqp.Queue) (err error) {
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	return
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		"topic-02", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)

	failOnError(err, "Failed to declare a queue")

	counter := 1

	// cron job
	for {
		msg := fmt.Sprintf(`{"Message" : "Hello World", "Num" : %v }`, counter)
		fmt.Println("Send :", msg)
		err = Publish(ch, q, []byte(msg))
		failOnError(err, "Failed publish message")

		time.Sleep(time.Second * 10)
		counter++
	}

	// goroutine
	//var mu sync.Mutex
	//var forever = make(chan struct{})
	//
	//for i := 1; i <= 1000; i++ {
	//	go func() {
	//		mu.Lock()
	//		msg := fmt.Sprintf(`{"Message" : "Hello World", "Number" : %v }`, counter)
	//		fmt.Println("Send :", msg)
	//		err = Publish(ch, q, []byte(msg))
	//		failOnError(err, "Failed publish message")
	//		counter++
	//		mu.Unlock()
	//	}()
	//}

	defer ch.Close()
	defer conn.Close()

	//<-forever

}
