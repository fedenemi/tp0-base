package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const connectRetries = 15
const connectRetryDelay = 1 * time.Second

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

type Bet struct {
	Agency    string
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

// Client Entity that encapsulates how
type Client struct {
	config   ClientConfig
	conn     net.Conn
	sigchan  chan os.Signal
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:  config,
		sigchan: make(chan os.Signal, 1),
	}
	signal.Notify(client.sigchan, syscall.SIGTERM)
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	for i := 0; i < connectRetries; i++ {
		conn, err := net.Dial("tcp", c.config.ServerAddress)
		if err == nil {
			c.conn = conn
			return nil
		}
		log.Debugf(
			"action: connect | result: in_progress | client_id: %v | error: %v",
			c.config.ID, err,
		)
		time.Sleep(connectRetryDelay)
	}
	err := fmt.Errorf("no se pudo conectar al servidor tras %d intentos", connectRetries)
	log.Criticalf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
	return err
}

func (c *Client) closeConn() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
	}
}

func (c *Client) StartClientLoop() {
	bet := Bet{
		Agency:    c.config.ID,
		FirstName: os.Getenv("NOMBRE"),
		LastName:  os.Getenv("APELLIDO"),
		Document:  os.Getenv("DOCUMENTO"),
		Birthdate: os.Getenv("NACIMIENTO"),
		Number:    os.Getenv("NUMERO"),
	}

	for {
		select {
		case <-c.sigchan:
			log.Infof("action: sigterm_received | result: success | client_id: %v", c.config.ID)
			return
		default:
		}

		if err := c.createClientSocket(); err != nil {
			return 
		}

		if err := SendBet(c.conn, bet); err != nil {
			log.Errorf("action: apuesta_enviada | result: error_transitorio | client_id: %v | error: %v",
				c.config.ID, err)
			c.closeConn()
			time.Sleep(connectRetryDelay)
			continue
		}

		resp, err := bufio.NewReader(c.conn).ReadString('\n')
		c.closeConn()
		if err != nil {
			time.Sleep(connectRetryDelay)
			continue
		}

		if resp == "OK\n" {
			log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v",
				bet.Document, bet.Number)
			return
		}

		log.Errorf("action: apuesta_enviada | result: fail | client_id: %v", c.config.ID)
		return
	}
}