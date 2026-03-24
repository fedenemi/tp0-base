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

func (c *Client) sendBatch(bets []Bet) error {
	if _, err := c.conn.Write([]byte{'B'}); err != nil {
		return err
	}

	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(bets)))
	if _, err := c.conn.Write(countBuf); err != nil {
		return err
	}

	for _, bet := range bets {
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
	}
	return nil
}

func (c *Client) notifyFin() error {
	if err := c.createClientSocket(); err != nil {
		return err
	}
	defer c.conn.Close()

	agencyID, _ := parseAgencyID(c.config.ID)
	buf := make([]byte, 5)
	buf[0] = 'F'
	binary.BigEndian.PutUint32(buf[1:], uint32(agencyID))
	if _, err := c.conn.Write(buf); err != nil {
		return err
	}

	resp, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		return err
	}
	if resp != "OK\n" {
		return fmt.Errorf("respuesta inesperada: %v", resp)
	}
	return nil
}

func (c *Client) queryWinners() ([]string, error) {
	for {
		if err := c.createClientSocket(); err != nil {
			return nil, err
		}

		agencyID, _ := parseAgencyID(c.config.ID)
		buf := make([]byte, 5)
		buf[0] = 'W'
		binary.BigEndian.PutUint32(buf[1:], uint32(agencyID))
		if _, err := c.conn.Write(buf); err != nil {
			c.conn.Close()
			return nil, err
		}

		header := make([]byte, 4)
		if _, err := readAll(c.conn, header); err != nil {
			c.conn.Close()
			return nil, err
		}

		if string(header) == "WAIT" {
			// por el \n
			extra := make([]byte, 1)
			readAll(c.conn, extra)
			c.conn.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		count := int(binary.BigEndian.Uint32(header))
		var winners []string
		for i := 0; i < count; i++ {
			lenBuf := make([]byte, 4)
			if _, err := readAll(c.conn, lenBuf); err != nil {
				c.conn.Close()
				return nil, err
			}
			dniLen := int(binary.BigEndian.Uint32(lenBuf))
			dniBuf := make([]byte, dniLen)
			if _, err := readAll(c.conn, dniBuf); err != nil {
				c.conn.Close()
				return nil, err
			}
			winners = append(winners, string(dniBuf))
		}
		c.conn.Close()
		return winners, nil
	}
}

func readAll(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func parseAgencyID(id string) (int, error) {
	var n int
	fmt.Sscanf(id, "%d", &n)
	return n, nil
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
			if c.conn != nil {
				c.conn.Close()
				log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
			}
			return
		default:
		}
        // Create the connection the server in every loop iteration. Send an
		if err := c.createClientSocket(); err != nil {
			return
		}

		if err := c.sendBatch(batch); err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			return
		}

		resp, err := bufio.NewReader(c.conn).ReadString('\n')
		c.conn.Close()
		if err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		if resp != "OK\n" {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v", c.config.ID)
			return
		}
	}

	if err := c.notifyFin(); err != nil {
		log.Errorf("action: fin_apuestas | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	winners, err := c.queryWinners()
	if err != nil {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", len(winners))
}