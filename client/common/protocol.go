package common

/*

Protocolo binario --> Cada mensaje comienza con 1 byte de tipo:
  'B' - Batch de apuestas
  'F' - Fin de apuestas de una agencia
  'W' - Consulta de ganadores

- 4 bytes big-endian.
- cada apuesta -> agency,first_name,last_name,document,birthdate,number\n (CSV)
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

// Protocolo: 1 byte 'B' + 4 bytes cantidad + [4 bytes longitud + CSV] * cantidad
func SendBatch(conn net.Conn, bets []Bet) error {
	if err := WriteAll(conn, []byte{'B'}); err != nil {
		return err
	}

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

// Protocolo: 1 byte 'F' + 4 bytes agency_id
func SendFin(conn net.Conn, agencyID int) error {
	buf := make([]byte, 5)
	buf[0] = 'F'
	binary.BigEndian.PutUint32(buf[1:], uint32(agencyID))
	return WriteAll(conn, buf)
}

// Protocolo: 1 byte 'W' + 4 bytes agency_id
func SendWinnersQuery(conn net.Conn, agencyID int) error {
	buf := make([]byte, 5)
	buf[0] = 'W'
	binary.BigEndian.PutUint32(buf[1:], uint32(agencyID))
	return WriteAll(conn, buf)
}

// Retorna nil, true si el servidor respondió WAIT (sorteo pendiente).
// Retorna lista de DNIs, false si el sorteo ya ocurrió.
func RecvWinners(conn net.Conn) ([]string, bool, error) {
	header := make([]byte, 4)
	if err := ReadAll(conn, header); err != nil {
		return nil, false, err
	}

	if string(header) == "WAIT" {
		extra := make([]byte, 1)
		ReadAll(conn, extra)
		return nil, true, nil
	}

	count := int(binary.BigEndian.Uint32(header))
	winners := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lenBuf := make([]byte, 4)
		if err := ReadAll(conn, lenBuf); err != nil {
			return nil, false, err
		}
		dniLen := int(binary.BigEndian.Uint32(lenBuf))
		dniBuf := make([]byte, dniLen)
		if err := ReadAll(conn, dniBuf); err != nil {
			return nil, false, err
		}
		winners = append(winners, string(dniBuf))
	}
	return winners, false, nil
}
