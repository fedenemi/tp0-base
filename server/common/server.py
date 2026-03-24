import socket
import logging
import signal
import threading
from common.utils import Bet, store_bets, load_bets, has_won


class Server:
    def __init__(self, port, listen_backlog, agencies_amount):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._agencies_amount = agencies_amount
        self._finished_agencies = set()
        self._lottery_done = False
        self._lock = threading.Lock()
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

    def __handle_sigterm(self, sig, frame):
        logging.info("action: sigterm_received | result: success")
        self._running = False
        self._server_socket.close()
        logging.info("action: close_server_socket | result: success")

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        threads = []
        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                t = threading.Thread(target=self.__handle_client_connection, args=(client_sock,))
                t.start()
                threads.append(t)
            except OSError:
                if not self._running:
                    break
                raise
        for t in threads:
            t.join()
        logging.info("action: server_shutdown | result: success")

    def __recv_all(self, sock, n):
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise OSError("connection closed")
            data += chunk
        return data

    def __handle_batch(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        raw_count = self.__recv_all(client_sock, 4)
        batch_count = int.from_bytes(raw_count, byteorder='big')

        bets = []
        for _ in range(batch_count):
            raw_len = self.__recv_all(client_sock, 4)
            msg_len = int.from_bytes(raw_len, byteorder='big')
            raw_msg = self.__recv_all(client_sock, msg_len)
            msg = raw_msg.decode('utf-8').strip()
            fields = msg.split(',')
            bet = Bet(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5])
            bets.append(bet)

        with self._lock:
            store_bets(bets)

        for bet in bets:
            logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')

        logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')
        client_sock.send(b'OK\n')

    def __handle_fin(self, client_sock):
        raw_agency = self.__recv_all(client_sock, 4)
        agency_id = int.from_bytes(raw_agency, byteorder='big')

        with self._lock:
            self._finished_agencies.add(agency_id)

            if len(self._finished_agencies) == self._agencies_amount:
                self._lottery_done = True
                logging.info("action: sorteo | result: success")

        client_sock.send(b'OK\n')

    def __handle_winners(self, client_sock):
        raw_agency = self.__recv_all(client_sock, 4)
        agency_id = int.from_bytes(raw_agency, byteorder='big')

        with self._lock:
            lottery_done = self._lottery_done

        if not lottery_done:
            client_sock.send(b'WAIT\n')
            return

        with self._lock:
            winners = [b for b in load_bets() if b.agency == agency_id and has_won(b)]

        count_buf = len(winners).to_bytes(4, byteorder='big')
        client_sock.send(count_buf)
        for w in winners:
            dni_bytes = w.document.encode('utf-8')
            client_sock.send(len(dni_bytes).to_bytes(4, byteorder='big'))
            client_sock.send(dni_bytes)

    def __handle_client_connection(self, client_sock):
        try:
            msg_type = self.__recv_all(client_sock, 1)

            if msg_type == b'B':
                self.__handle_batch(client_sock)
            elif msg_type == b'F':
                self.__handle_fin(client_sock)
            elif msg_type == b'W':
                self.__handle_winners(client_sock)
        except OSError:
            pass
        except Exception as e:
            logging.error(f'action: apuesta_recibida | result: fail | cantidad: 0 | error: {e}')
            try:
                client_sock.send(b'ERROR\n')
            except:
                pass
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
