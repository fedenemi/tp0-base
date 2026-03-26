package common

/*
Módulo de protocolo de comunicación entre cliente y servidor.

Protocolo binario. Formato de batch:
  4 bytes cantidad + [4 bytes longitud + datos CSV] * cantidad

Formato CSV: agency,first_name,last_name,document,birthdate,number\n
*/

import (
	"encoding/binary"
	"fmt"
	"net"
)

// ReadAll lee exactamente len(buf) bytes del socket, evitando short-reads.
func ReadAll(conn net.Conn, buf []byte) error {
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

// WriteAll escribe exactamente len(buf) bytes en el socket, evitando short-writes.
func WriteAll(conn net.Conn, buf []byte) error {
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

// SendBatch envía un batch de apuestas al servidor.
// Protocolo: 4 bytes cantidad + [4 bytes longitud + CSV] * cantidad
func SendBatch(conn net.Conn, bets []Bet) error {
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(bets)))
	if err := WriteAll(conn, countBuf); err != nil {
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
		if err := WriteAll(conn, lenBuf); err != nil {
			return err
		}
		if err := WriteAll(conn, msgBytes); err != nil {
			return err
		}
	}
	return nil
}
