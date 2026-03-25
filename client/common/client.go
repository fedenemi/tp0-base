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

const connectRetries = 5
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

func readAll(conn net.Conn, buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return err
		}
	}
	return nil
}

func writeAll(conn net.Conn, buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := conn.Write(buf[total:])
		total += n
		if err != nil {
			return err
		}
	}
	return nil
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
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID, err,
		)
		time.Sleep(connectRetryDelay)
	}
	return fmt.Errorf("no se pudo conectar al servidor tras %d intentos", connectRetries)
}

func (c *Client) closeConn() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
	}
}

// sendBet envía una apuesta al servidor.
// Protocolo: 4 bytes longitud + datos CSV
func (c *Client) sendBet(bet Bet) error {
	msg := fmt.Sprintf("%s,%s,%s,%s,%s,%s\n",
		bet.Agency, bet.FirstName, bet.LastName,
		bet.Document, bet.Birthdate, bet.Number,
	)

	msgBytes := []byte(msg)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(msgBytes)))

	if err := writeAll(c.conn, lenBuf); err != nil {
		return err
	}
	if err := writeAll(c.conn, msgBytes); err != nil {
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
	if err := c.createClientSocket(); err != nil {
		return
	}
	defer c.closeConn()

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