import socket
import logging
import signal
from common.utils import Bet, store_bets

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
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
        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
            except OSError:
                if not self._running:
                    break
                raise
        logging.info("action: server_shutdown | result: success")

    def __recv_all(self, sock, n):
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise OSError("connection closed")
            data += chunk
        return data

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """

        try:
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

            store_bets(bets)
            for bet in bets:
                logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')

            logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')
            client_sock.send(b'OK\n')
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
