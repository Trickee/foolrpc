package foolrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

// Server Server
type Server struct {
	s *rpc.Server
}

// NewServer NewServer
func NewServer() *Server {
	return &Server{
		s: rpc.NewServer(),
	}
}

// Register Register
func (server *Server) Register(rcvr interface{}) error {
	return server.s.Register(rcvr)
}

// RegisterName RegisterName
func (server *Server) RegisterName(name string, rcvr interface{}) error {
	return server.s.RegisterName(name, rcvr)
}

// Serve Serve
func (server *Server) Serve(url string, queue string) {
	if conn, err := amqp.Dial(url); err != nil {
		failOnError(err, "Failed to connect to MQServer")
	} else {
		server.ServeConn(conn, queue)
	}
}

// ServeConn ServeConn
func (server *Server) ServeConn(conn *amqp.Connection, queue string) {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q, err := ch.QueueDeclare(
		queue, // name
		false, // durable
		true,  // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // autoAck
		false,  // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // arguments
	)
	failOnError(err, "Failed to register a consumer")
	codec := &serverCodec{
		conn:    conn,
		ch:      ch,
		msgs:    msgs,
		pending: make(map[uint64]prop),
	}
	server.s.ServeCodec(codec)
}

type serverCodec struct {
	sync.Mutex
	conn    *amqp.Connection
	ch      *amqp.Channel
	msgs    <-chan amqp.Delivery
	req     serverRequest
	seq     uint64
	pending map[uint64]prop
}

type serverRequest struct {
	Method string           `json:"method"`
	Params *json.RawMessage `json:"params"`
}

func (r *serverRequest) reset() {
	r.Method = ""
	r.Params = nil
}

type serverResponse struct {
	Result interface{} `json:"result"`
	Error  interface{} `json:"error"`
}

type prop struct {
	correlationID string
	replyTo       string
}

func (c *serverCodec) ReadRequestHeader(r *rpc.Request) error {

	msg := <-c.msgs
	c.req.reset()
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err := json.Unmarshal(msg.Body, &c.req); err != nil {
		return err
	}
	r.ServiceMethod = c.req.Method
	c.Lock()
	c.seq++
	c.pending[c.seq] = prop{correlationID: msg.CorrelationId, replyTo: msg.ReplyTo}
	r.Seq = c.seq
	c.Unlock()

	return nil
}

func (c *serverCodec) ReadRequestBody(body interface{}) error {
	if body == nil {
		return nil
	}
	if c.req.Params == nil {
		return errors.New("mqrpc: request body missing params")
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	return json.Unmarshal(*c.req.Params, body)
}

func (c *serverCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	var resp serverResponse
	c.Lock()
	prop, ok := c.pending[r.Seq]
	if !ok {
		c.Unlock()
		return errors.New("invalid sequence number in response")
	}
	delete(c.pending, r.Seq)
	c.Unlock()

	resp.Result = body
	if r.Error == "" {
		resp.Error = nil
	} else {
		resp.Error = r.Error
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&resp)
	if err != nil {
		return err
	}
	return c.ch.Publish(
		"",           // exchange
		prop.replyTo, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: prop.correlationID,
			Body:          b,
		},
	)
}

func (c *serverCodec) Close() error {
	return c.conn.Close()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
