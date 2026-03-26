import socket
import logging
import signal
from common.utils import Bet, store_bets
from common import protocol

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

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            msg = protocol.recv_bet(client_sock)
            if not msg:
                return

            fields = msg.split(',')
            if len(fields) < 6:
                return

            bet = Bet(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5])
            store_bets([bet])
            logging.info(
                f'action: apuesta_almacenada | result: success | '
                f'dni: {bet.document} | numero: {bet.number}'
            )
            protocol.send_ok(client_sock)
        except OSError:
            pass
        finally:
            client_sock.close()
            logging.info("action: close_client_socket | result: success")

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
