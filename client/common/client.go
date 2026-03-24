package common

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

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
	for retries := 0; retries < 5; retries++ {
		conn, err := net.Dial("tcp", c.config.ServerAddress)
		if err == nil {
			c.conn = conn
			return nil
		}
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID, err,
		)
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("no se pudo conectar al servidor")
}

func (c *Client) sendBet(bet Bet) error {
	msg := fmt.Sprintf("%s,%s,%s,%s,%s,%s\n",
		bet.Agency, bet.FirstName, bet.LastName,
		bet.Document, bet.Birthdate, bet.Number,
	)

	msgBytes := []byte(msg)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msgBytes)))

	if _, err := c.conn.Write(lenBuf); err != nil {
		return err
	}
	if _, err := c.conn.Write(msgBytes); err != nil {
		return err
	}
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	bet := Bet{
		Agency:    c.config.ID,
		FirstName: os.Getenv("NOMBRE"),
		LastName:  os.Getenv("APELLIDO"),
		Document:  os.Getenv("DOCUMENTO"),
		Birthdate: os.Getenv("NACIMIENTO"),
		Number:    os.Getenv("NUMERO"),
	}

	select {
	case <-c.sigchan:
		log.Infof("action: sigterm_received | result: success | client_id: %v", c.config.ID)
		return
	default:
	}
		// Create the connection the server in every loop iteration. Send an
	err := c.createClientSocket()
	if err != nil {
		return
	}
	defer func() {
		c.conn.Close()
		log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
	}()

	if err := c.sendBet(bet); err != nil {
		log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
		return
	}

	resp, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
		return
	}

	if resp == "OK\n" {
		log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v",
			bet.Document, bet.Number)
	} else {
		log.Errorf("action: apuesta_enviada | result: fail | client_id: %v", c.config.ID)
	}
}