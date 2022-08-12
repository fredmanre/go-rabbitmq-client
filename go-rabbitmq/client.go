package rabbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQClient contains the basic objects for a AMQ connection
type RabbitMQClient struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Reception  struct {
		Queue amqp.Queue
	}
	Dispatch struct{}
	Err      chan error
}

// ################################## Reconneciton config
type RabbitConnection struct {
	username            string
	password            string
	host                string
	port                int `default:"5672"`
	ReconnectionTimeout int `default:"5"`
	SetupQueue          RabbitSetupQueue
}

type RabbitSetupQueue struct {
	exchange       string
	queueName      string
	autoDelete     bool
	routingKeys    []string
	queueIsDurable bool
}

var RbConn RabbitConnection

// rbp RabbitParameters
func (rbp *RabbitConnection) InitMeConn(username, password, host string, port int) {
	rbp.username, rbp.password, rbp.host = username, password, host
}

func (rbp *RabbitConnection) InitMeSetupQueue(exchange, queueName string, autoDelete, isDurable bool, routingKeys []string) {
	rbp.SetupQueue.exchange, rbp.SetupQueue.queueName = exchange, queueName
	rbp.SetupQueue.autoDelete, rbp.SetupQueue.queueIsDurable, rbp.SetupQueue.routingKeys = autoDelete, isDurable, routingKeys
}

// #####################################################################

// RabbitMQClient_GetQueueName return the queue name for a receiver
func (a *RabbitMQClient) GetQueueName() string {
	return a.Reception.Queue.Name
}

// RabbitMQClient_StartConnection Starts the connection with rabbitMQ server.
// Dials up and creates a channel
func (a *RabbitMQClient) StartConnection(username, password, host string, port int) error {

	a.Err = make(chan error)
	RbConn.InitMeConn(username, password, host, port)
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", username, password, host, port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("amqp dial failure: %s", err)
	}
	// we declare a channel to be used for the reconnection process
	go func() {
		<-a.Connection.NotifyClose(make(chan *amqp.Error)) // we listen to notify if the connection is closed
		a.Err <- errors.New("disconnected from rabbitMQ")
	}()

	a.Connection = conn
	errCh := a.CreateChannel()
	if errCh != nil {
		return fmt.Errorf("couldn't create a channel in start connection err: %s", errCh)
	}
	return nil
}

// RabbitMQClient_CreateChannel creates a channel and saves it in struct
func (a *RabbitMQClient) CreateChannel() error {
	ch, err := a.Connection.Channel()
	if err != nil {
		return fmt.Errorf("channel creating failure: %s", err)
	}
	a.Channel = ch
	return nil
}

// Manage reconnection to rabbitMQ
// for startConnection: username, password, host & port
// for SetupQueues: queueName, queueIsDurable, autoDelete, routingKeys, exchange
func (a *RabbitMQClient) Reconnect() error {

	time.Sleep(time.Duration(RbConn.ReconnectionTimeout))
	fmt.Println("Recconnecting with params ", RbConn)
	RbConn.ReconnectionTimeout += 10
	if err := a.StartConnection(RbConn.username, RbConn.password, RbConn.host, RbConn.port); err != nil {
		return err
	}
	if err := a.SetupQueues(
		RbConn.SetupQueue.queueName, RbConn.SetupQueue.queueIsDurable,
		RbConn.SetupQueue.autoDelete, RbConn.SetupQueue.routingKeys,
		RbConn.SetupQueue.exchange); err != nil {
		return err
	}
	return nil

}

// RabbitMQClient_SetupQueues Declares and binds a queue to an exchange
func (a *RabbitMQClient) SetupQueues(queueName string, queueIsDurable, autoDelete bool, routingKeys []string, exchange string) error {
	q, err := a.Channel.QueueDeclare(
		queueName,      // name
		queueIsDurable, // durable
		autoDelete,     // delete when un used
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue with name: %s, err: %s", queueName, err)
	}
	RbConn.InitMeSetupQueue(exchange, queueName, autoDelete, queueIsDurable, routingKeys) // params for reconnection queue
	for _, key := range routingKeys {
		errBind := a.Channel.QueueBind(queueName, key, exchange, false, nil)
		if errBind != nil {
			return fmt.Errorf("failed to Bind a queue err: %s", errBind)
		}
	}

	a.Reception.Queue = q
	return nil
}

// RabbitMQClient_StartReceiver Starts a rabbit MQ receiver with the passed configuration, returns a channel
// that will receive the messages, along with the connection and channel instance
func (a *RabbitMQClient) StartReceiver(queueName string, isDurable, autoDelete bool, routingKeys []string, exchanges interface{}, consumerTag string) (<-chan amqp.Delivery, error) {
	switch exchangeData := exchanges.(type) {
	case []string:
		for _, exchange := range exchangeData {
			err := a.SetupQueues(
				queueName, isDurable, autoDelete, routingKeys, exchange,
			)
			if err != nil {
				return make(chan amqp.Delivery), fmt.Errorf("failed to setup queue err: %s", err)
			}
		}
	case string:
		err := a.SetupQueues(
			queueName, isDurable, autoDelete, routingKeys, exchangeData,
		)
		if err != nil {
			return make(chan amqp.Delivery), fmt.Errorf("failed to setup queue err: %s", err)
		}

	default:
		return make(chan amqp.Delivery), fmt.Errorf("failed to declare exchange due to wrong type in var %v", exchanges)
	}
	messages, err := a.Channel.Consume(
		queueName,   // queue
		consumerTag, // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return make(chan amqp.Delivery), fmt.Errorf("failed to register a consumer: %s", err)
	}
	return messages, nil
}

// RabbitMQClient_SetupDispatcher Declares the exchanges to be used to deliver messages
func (a *RabbitMQClient) SetupDispatcher(exchange, exchangeType string, isDurable, autoDelete bool) error {
	if err := a.Channel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		isDurable,    // durable
		autoDelete,   // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("exchange declare: %s", err)
	}
	return nil
}

// RabbitMQClient_SendMessage Deliver the message to the specified exchange, if exchange not created this will
// throw an error
func (a *RabbitMQClient) SendMessage(exchange, routingKey string, message interface{}) error {
	// non blocking channel - if there is no error will go to default where we do nothing
	select {
	case err := <-a.Err:
		if err != nil {
			a.Reconnect()
		}
	default:
	}

	messageBody, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("couldn't marshal the map to an slice of bytes")
	}

	if err = a.Channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            messageBody,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		},
	); err != nil {
		return fmt.Errorf("exchange publish: %s", err)
	}
	return nil
}
