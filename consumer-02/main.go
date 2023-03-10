package main

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"os"
	"time"

	"log"
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

var num int = 1

func Consume(ch *amqp.Channel, q amqp.Queue) (err error) {

	f, err := os.OpenFile("./fail.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	loc, _ := time.LoadLocation("Asia/Jakarta")

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
		jsonMsg := Message{}
		err = json.Unmarshal(d.Body, &jsonMsg)
		if err != nil {
			fmt.Println(err)
		}

		if jsonMsg.Num == num {
			log.Printf("Received a message: %s", d.Body)
		} else {
			data := History{
				Time: time.Now().In(loc).Format("15:04:05"),
				Num:  num,
			}
			byteArray, err := json.Marshal(data)
			if err != nil {
				fmt.Println(err)
			}
			if _, err := fmt.Fprintf(f, "%s\n", byteArray); err != nil {
				fmt.Println(err)
			}
			n, err := io.WriteString(f, string(byteArray))
			if err != nil {
				fmt.Println(n, err)
			}

		}
		num++

	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	return
}

type Message struct {
	Message string
	Num     int
}

type History struct {
	Time string
	Num  int
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		"topic-03", // name
		false,      // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)

	failOnError(err, "Failed to declare a queue")

	var forever = make(chan struct{})
	err = Consume(ch, q)
	failOnError(err, "Failed consume message")

	defer ch.Close()
	defer conn.Close()

	<-forever
}
