/// Example requires amqp broker and redis backend
package main

import (
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v2"
	"github.com/RichardKnop/machinery/v2/backends/redis"
	"github.com/RichardKnop/machinery/v2/brokers/amqp"
	"github.com/RichardKnop/machinery/v2/tasks"
)

type Message struct {
	Message string `json:"message"`
}

func atask(msg Message) (Message, error) {
	log.Printf("Got message: %+v", msg)
	return Message{Message: "Return message"}, nil
}

func main() {
	conf := &config.Config{
		Broker:          "amqp://guest:guest@localhost:5672/",
		DefaultQueue:    "machinery_tasks",
		ResultBackend:   "redis://localhost:6379/1",
		ResultsExpireIn: 3600, // Expire results in 1hr
		AMQP: &config.AMQPConfig{
			Exchange:     "machinery_exchange",
			ExchangeType: "direct",
			BindingKey:   "machinery_task",
		},
	}
	broker := amqp.New(conf)
	backend := redis.New(conf, "localhost:6379", "", "", 0)

	server := machinery.NewServer(conf, broker, backend)

	err := server.RegisterTasks(map[string]interface{}{
		"atask": atask,
	})
	if err != nil {
		panic(err)
	}

	worker := server.NewWorker("worker1", 10)

	asyncResult, err := server.SendTask(tasks.NewSignature("atask", Message{Message: "foo"}))
	if err != nil {
		panic(err)
	}

	ec := make(chan error, 100)
	worker.LaunchAsync(ec)

	result, err := asyncResult.GetWithTimeout(10*time.Second, 100*time.Millisecond)
	if err != nil {
		panic(err)
	}
	log.Printf("Got results: %+v", result)

	select {
	case err := <-ec:
		log.Fatalf("Worker threw error: %s", err)
	default:
	}
}
