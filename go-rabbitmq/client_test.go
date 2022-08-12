package rabbit

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	username = os.Getenv("RABBIT_USER")
	password = os.Getenv("RABBIT_PWD")
	host     = os.Getenv("RABBIT_HOST")
	port     = 5672
)

// HELPERS FOR TESTING PURPOSES
func getConn() (*RabbitMQClient, error) {
	client := &RabbitMQClient{}
	log.Println(username, password, host, port)
	err := client.StartConnection(username, password, host, port)
	if err != nil {
		return client, err
	}
	err = client.CreateChannel()
	return client, err
}

func getQueues(
	client *RabbitMQClient,
	queueName string,
	isDurable,
	autoDelete bool,
	routingKeys []string,
	exchange string,
) (<-chan amqp.Delivery, error) {

	err := client.SetupQueues(
		queueName,
		isDurable,
		autoDelete,
		routingKeys,
		exchange,
	)
	if err != nil {
		return make(chan amqp.Delivery), err
	}
	messages, err := client.Channel.Consume(
		client.Reception.Queue.Name, // queue
		exchange,                    // consumer
		true,                        // auto-ack
		false,                       // exclusive
		false,                       // no-local
		false,                       // no-wait
		nil,                         // args
	)

	return messages, err
}

func runDispatching() {
	// Regular testing, should work just fine
	client, err := getConn()
	if err != nil {
		log.Fatalf("Failure getting client to connect: %s", err)
	}
	err = client.SetupDispatcher(
		"test",
		"fanout",
		false,
		false,
	)
	if err != nil {
		log.Fatalf("dispatcher: %s", err)
	}

	for {
		err = client.SendMessage(
			"test",
			"",
			map[string]interface{}{
				"hola": "bye",
				"num":  21231,
			},
		)
		time.Sleep(1 * time.Second)
	}
}

func TestRabbitMQClient_GetQueueName(t *testing.T) {
	t.Parallel()
	client := &RabbitMQClient{}
	client.Reception.Queue.Name = "test"
	if client.GetQueueName() != "test" {
		t.Error("Error getting or setting queue name")
	}
}

// The dial that connects to RabbitMQ server
func TestRabbitMQClient_StartConnection(t *testing.T) {
	t.Parallel()
	_, err := getConn()
	if err != nil {
		t.Errorf("Connection failed, error: %s", err)
	}

	client := &RabbitMQClient{}

	err = client.StartConnection(
		"aasd",
		"asdasd",
		"12312123.123",
		5672,
	)
	if err == nil {
		t.Error("Error should arise in starting connection")
	}
}

// Test the channel creation
func TestRabbitMQClient_StartChannel(t *testing.T) {
	t.Parallel()
	client, err := getConn()
	if err != nil {
		t.Errorf("Connection failed, error: %s", err)
	}
	errConn := client.Connection.Close()
	if errConn != nil {
		t.Errorf("Error closing the connection, err: %s", errConn)
	}
	err = client.CreateChannel()
	if err == nil {
		t.Errorf("%s", err)
	}
}

// Test the setup and consume methods that creates a queue and consumes from it
func TestRabbitMQClient_SetupQueues(t *testing.T) {
	t.Parallel()
	client, err := getConn()
	if err != nil {
		t.Errorf("Connection failed, error: %s", err)
	}

	err = client.SetupDispatcher(
		"test",
		"fanout",
		false,
		false,
	)
	if err != nil {
		t.Errorf("Connection failed, error: %s", err)
	}
	_, err = getQueues(
		client,
		"client_test",
		false,
		false,
		[]string{""},
		"test",
	)

	if err != nil {
		t.Errorf("Error setting up Queue declaring and consume channel")
	}
	_, err = getQueues(
		client,
		"client_test",
		false,
		true,
		[]string{""},
		"test",
	)
	if err == nil {
		t.Errorf("Should throw error because of arguments difference")
	}

	_, err = getQueues(
		client,
		"client_test",
		false,
		false,
		[]string{"1231asda"},
		"test_&./~??=+~!",
	)
	if err == nil {
		t.Errorf("Should say that the queue doesn't exist")
	}
}

// Test the full receiver component
func TestRabbitMQClient_StartReceiver(t *testing.T) {
	t.Parallel()
	client, err := getConn()
	if err != nil {
		t.Errorf("Failure getting client to connect: %s", err)
	}
	_, err = client.StartReceiver(
		"client_test",
		false,
		false,
		[]string{""},
		"test",
		"",
	)
	if err != nil {
		t.Errorf("Failure starting receiver: %s", err)
	}
	_, err = client.StartReceiver(
		"client_test",
		false,
		true,
		[]string{""},
		"test",
		"",
	)
	if err == nil {
		t.Errorf("failure should arise in receiver: %s", err)
	}
	_, err = client.StartReceiver(
		"client_test",
		false,
		false,
		[]string{""},
		"test_1",
		"",
	)
	if err == nil {
		t.Errorf("failure should arise in receiver: %s", err)
	}

}

// Test a full dispatch operation, from exchange declaring to message delivery
func TestRabbitMQClient_SetupDispatcher(t *testing.T) {
	t.Parallel()
	// Regular testing, should work just fine
	client, err := getConn()
	if err != nil {
		t.Errorf("Failure getting client to connect: %s", err)
	}
	err = client.SetupDispatcher(
		"test",
		"fanout",
		false,
		false,
	)
	if err != nil {
		t.Errorf("dispatcher: %s", err)
	}
	err = client.SendMessage(
		"test",
		"",
		map[string]interface{}{
			"hola": "bye",
			"num":  21231,
		},
	)
	if err != nil {
		t.Errorf("sending message: %s", err)
	}

	// Setting up existing exchange with different configuration
	err = client.SetupDispatcher(
		"amq.match",
		"topic",
		false,
		true,
	)
	if err == nil {
		t.Errorf("dispatcher should fail: %s", err)
	}

	// Sending message to wrong exchange
	t.Log("Sending message...")
	err = client.SendMessage(
		"amq.match",
		"adas",
		map[string]interface{}{
			"hola": "bye",
			"num":  21231,
		},
	)
	if err == nil {
		t.Errorf("Sending message to wrong destiny: %s", err)
	}

	// Sending wrong message content
	t.Log("Sending message...")
	err = client.SendMessage(
		"amq.match",
		"",
		map[string]interface{}{
			"hola": "bye",
			"num": func(x, y float64) float64 {
				return x + y
			},
		},
	)
	if err == nil {
		t.Errorf("Sending wrong message to destiny: %s", err)
	}

}

// Checks the exchange used actually exist on a server
func ExampleRabbitMQClient_StartReceiver() {

	go runDispatching()
	client, err := getConn()
	if err != nil {
		log.Fatalf("Failure getting client to connect: %s", err)
	}
	messageChannel, err := client.StartReceiver(
		"client_test",
		false,
		false,
		[]string{""},
		"test",
		"",
	)
	if err != nil {
		log.Fatalf("Failure starting receive: %s", err)
	}
	c := <-messageChannel
	fmt.Println(c.Exchange)
	// Output:
	// test
}

// Test how much starting the receiver and receiving a message takes
func BenchmarkRabbitMQClient_StartReceiver(b *testing.B) {

	// Starts dispatch for testing purposes
	go runDispatching()

	client, err := getConn()
	if err != nil {
		log.Fatalf("Failure getting client to connect: %s", err)
	}
	messageChannel, err := client.StartReceiver(
		"client_test",
		false,
		false,
		[]string{""},
		"test",
		"",
	)
	if err != nil {
		log.Fatalf("Failure starting receive: %s", err)
	}
	c := <-messageChannel
	fmt.Println(c.Exchange)
}

func TestRecconnectingRabbitMQClient_Reconnect(t *testing.T) {
	t.Parallel()
	// Starts dispatch for testing purposes
	go runDispatching()

	client, err := getConn()
	if err != nil {
		log.Fatalf("Failure getting client to connect: %s", err)
	}
	Err := client.Reconnect()
	if Err != nil {
		log.Fatalf("Failure to reconnect: %s", err)
	}
	fmt.Println(Err)
}
