package foolrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pborman/uuid"
	"github.com/streadway/amqp"
)

var rpcTimeout int64 = 20

// Client Client
type Client struct {
	url       string
	mu        sync.Mutex
	conn      *amqp.Connection
	clientMap map[string]*rpc.Client
}

// Dial Dial
func Dial(url string) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.New("Failed to connect to MQServer: " + err.Error())
	}
	return NewClientWithConn(conn, url), nil
}

// NewClientWithConn NewClientWithConn
func NewClientWithConn(conn *amqp.Connection, url string) *Client {
	return &Client{
		url:       url,
		conn:      conn,
		clientMap: make(map[string]*rpc.Client),
	}
}

//reconn to MQ Server
func (client *Client) reconn() {
	if conn, err := amqp.Dial(client.url); err != nil {
		log.Println(err)
	} else {
		log.Println("reconn")
		client.conn = conn
	}
}

// JSONCall Call
func (client *Client) JSONCall(queue string, serviceMethod string, args *[]byte, reply *[]byte, isChildCall ...bool) error {
	c, err := client.jsonClient(queue)
	if err != nil {
		return err
	}
	timeout := time.NewTimer(time.Second * time.Duration(rpcTimeout))
	select {
	case call := <-c.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1)).Done:
		if call.Error == rpc.ErrShutdown && !(len(isChildCall) == 1 && isChildCall[0] == true) {
			client.mu.Lock()
			delete(client.clientMap, queue)
			client.mu.Unlock()
			return client.JSONCall(queue, serviceMethod, args, reply, true)
		}
		return call.Error
	case <-timeout.C: //3s timeout
		return fmt.Errorf("timeout %ds", rpcTimeout)
	}
}

func (client *Client) jsonClient(queue string) (*rpc.Client, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	c, ok := client.clientMap[queue]
	if ok {
		return c, nil
	}
	ch, err := client.conn.Channel()
	if err != nil {
		client.reconn()
		if ch, err = client.conn.Channel(); err != nil {
			return nil, errors.New("Failed to open a channel")
		}
	}
	if q, err := ch.QueueInspect(queue); err != nil && q.Consumers == 0 {
		return nil, errors.New("No such service: " + queue)
	}
	q, err := ch.QueueDeclare(
		strings.Replace(os.Args[0], "./", "", -1)+"."+queue+"."+uuid.New(), // name
		false, // durable
		true,  // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, errors.New("Failed to declare a queue")
	}
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // autoAck
		false,  // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // arguments
	)
	if err != nil {
		return nil, errors.New("Failed to register a consumer")
	}
	codec := &jsonClientCodec{
		queue:     queue,
		replyTo:   q.Name,
		ch:        ch,
		msgs:      msgs,
		pending:   make(map[uint64]string),
		clientMap: client.clientMap,
	}
	c = rpc.NewClientWithCodec(codec)
	client.clientMap[queue] = c
	return c, nil
}

type jsonClientCodec struct {
	sync.Mutex
	queue     string
	replyTo   string
	req       jsonClientRequest
	resp      jsonClientResponse
	ch        *amqp.Channel
	msgs      <-chan amqp.Delivery
	pending   map[uint64]string
	clientMap map[string]*rpc.Client
}

type jsonClientRequest struct {
	Method string           `json:"method"`
	Params *json.RawMessage `json:"params"`
}

type jsonClientResponse struct {
	Result *json.RawMessage `json:"result"`
	Error  interface{}      `json:"error"`
}

func (r *jsonClientResponse) reset() {
	r.Result = nil
	r.Error = nil
}

func (c *jsonClientCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	c.Lock()
	c.pending[r.Seq] = r.ServiceMethod
	c.Unlock()
	c.req.Method = r.ServiceMethod
	params, _ := body.(*[]byte)
	paramsRawMessage := json.RawMessage(*params)
	c.req.Params = &paramsRawMessage

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&c.req)
	if err != nil {
		return err
	}
	return c.ch.Publish(
		"",      // exchange
		c.queue, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: strconv.FormatUint(r.Seq, 10),
			ReplyTo:       c.replyTo,
			Body:          b,
		},
	)
}

func (c *jsonClientCodec) ReadResponseHeader(r *rpc.Response) error {
	timeout := time.NewTimer(time.Second * 3600)
	select {
	case msg := <-c.msgs:
		c.resp.reset()
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		if err := json.Unmarshal(msg.Body, &c.resp); err != nil {
			return err
		}
		seq, err := strconv.ParseUint(msg.CorrelationId, 0, 64)
		if err != nil {
			return err
		}
		c.Lock()
		r.Seq = seq
		r.ServiceMethod = c.pending[seq]
		delete(c.pending, seq)
		c.Unlock()

		r.Error = ""
		if c.resp.Error != nil || c.resp.Result == nil {
			x, ok := c.resp.Error.(string)
			if !ok {
				return fmt.Errorf("invalid error %v", c.resp.Error)
			}
			if x == "" {
				x = "unspecified error"
			}
			r.Error = x
		}
	case <-timeout.C: //clear queue when 1 hour no message
		c.Close()
		return errors.New("timeout 3600")
	}
	return nil
}

func (c *jsonClientCodec) ReadResponseBody(body interface{}) error {
	if body != nil {
		bodyReal := body.(*[]byte)
		*bodyReal = *c.resp.Result
	}
	return nil
}

func (c *jsonClientCodec) Close() error {
	delete(c.clientMap, c.queue)
	return c.ch.Close()
}
