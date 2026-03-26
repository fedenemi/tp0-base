package common

/*
Protocolo binario -->  4 bytes longitud + datos CSV
Formato CSV: agency,first_name,last_name,document,birthdate,number\n
*/

import (
	"encoding/binary"
	"fmt"
	"net"
)

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

// Protocolo: 4 bytes longitud + datos CSV
func SendBet(conn net.Conn, bet Bet) error {
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
	return WriteAll(conn, msgBytes)
}
