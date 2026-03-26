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

const connectRetries = 5
const connectRetryDelay = 1 * time.Second

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchMaxAmount int
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

func (c *Client) readBets() ([][]Bet, error) {
	file, err := os.Open("/agency.csv")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var batches [][]Bet
	var current []Bet

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		fields := splitCSV(line)
		if len(fields) < 5 {
			continue
		}
		bet := Bet{
			Agency:    c.config.ID,
			FirstName: fields[0],
			LastName:  fields[1],
			Document:  fields[2],
			Birthdate: fields[3],
			Number:    fields[4],
		}
		current = append(current, bet)
		if len(current) >= c.config.BatchMaxAmount {
			batches = append(batches, current)
			current = nil
		}
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches, scanner.Err()
}

func splitCSV(line string) []string {
	var fields []string
	var current []byte
	for _, ch := range line {
		if ch == ',' {
			fields = append(fields, string(current))
			current = nil
		} else {
			current = append(current, byte(ch))
		}
	}
	fields = append(fields, string(current))
	return fields
}

func (c *Client) StartClientLoop() {
	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	batches, err := c.readBets()
	if err != nil {
		log.Errorf("action: read_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	for _, batch := range batches {
		select {
		case <-c.sigchan:
			log.Infof("action: sigterm_received | result: success | client_id: %v", c.config.ID)
			c.closeConn()
			return
		default:
		}
        // Create the connection the server in every loop iteration. Send an
		if err := c.createClientSocket(); err != nil {
			return
		}

		if err := SendBatch(c.conn, batch); err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.closeConn()
			return
		}

		resp, err := bufio.NewReader(c.conn).ReadString('\n')
		c.closeConn()
		if err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		if resp != "OK\n" {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v", c.config.ID)
			return
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}