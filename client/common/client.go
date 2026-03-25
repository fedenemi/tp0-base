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


// Protocolo: 1 byte tipo 'B' + 4 bytes cantidad + por cada apuesta: 4 bytes longitud + datos
func (c *Client) sendBatch(bets []Bet) error {
	if err := writeAll(c.conn, []byte{'B'}); err != nil {
		return err
	}

	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(bets)))
	if err := writeAll(c.conn, countBuf); err != nil {
		return err
	}

	// Cada apuesta: longitud + datos
	for _, bet := range bets {
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
	}
	return nil
}

// Protocolo: 1 byte tipo 'F' + 4 bytes agency_id
func (c *Client) notifyFin() error {
	if err := c.createClientSocket(); err != nil {
		return err
	}
	defer c.closeConn()

	agencyID, _ := parseAgencyID(c.config.ID)
	buf := make([]byte, 5)
	buf[0] = 'F'
	binary.BigEndian.PutUint32(buf[1:], uint32(agencyID))
	if err := writeAll(c.conn, buf); err != nil {
		return err
	}

	resp, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		return err
	}
	if resp != "OK\n" {
		return fmt.Errorf("respuesta inesperada del servidor: %v", resp)
	}
	return nil
}

// Protocolo: 1 byte tipo 'W' + 4 bytes agency_id
// Respuesta: 'WAIT\n' si el sorteo no ocurrió, o 4 bytes cantidad + DNIs
func (c *Client) queryWinners() ([]string, error) {
	for {
		if err := c.createClientSocket(); err != nil {
			return nil, err
		}

		agencyID, _ := parseAgencyID(c.config.ID)
		buf := make([]byte, 5)
		buf[0] = 'W'
		binary.BigEndian.PutUint32(buf[1:], uint32(agencyID))
		if err := writeAll(c.conn, buf); err != nil {
			c.closeConn()
			return nil, err
		}

		header := make([]byte, 4)
		if err := readAll(c.conn, header); err != nil {
			c.closeConn()
			return nil, err
		}

		if string(header) == "WAIT" {
			extra := make([]byte, 1) //  \n
			readAll(c.conn, extra)
			c.closeConn()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		count := int(binary.BigEndian.Uint32(header))
		winners := make([]string, 0, count)
		for i := 0; i < count; i++ {
			lenBuf := make([]byte, 4)
			if err := readAll(c.conn, lenBuf); err != nil {
				c.closeConn()
				return nil, err
			}
			dniLen := int(binary.BigEndian.Uint32(lenBuf))
			dniBuf := make([]byte, dniLen)
			if err := readAll(c.conn, dniBuf); err != nil {
				c.closeConn()
				return nil, err
			}
			winners = append(winners, string(dniBuf))
		}
		c.closeConn()
		return winners, nil
	}
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
			c.closeConn()
			return
		default:
		}
// Create the connection the server in every loop iteration. Send an
		if err := c.createClientSocket(); err != nil {
			return
		}

		if err := c.sendBatch(batch); err != nil {
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