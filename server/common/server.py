import socket
import logging
import signal
from common.utils import Bet, store_bets, load_bets, has_won
from common import protocol


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

    def __handle_batch(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        messages = protocol.recv_batch(client_sock)

        bets = []
        for msg in messages:
            fields = msg.split(',')
            bet = Bet(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5])
            bets.append(bet)

        store_bets(bets)
        for bet in bets:
            logging.info(
                f'action: apuesta_almacenada | result: success | '
                f'dni: {bet.document} | numero: {bet.number}'
            )
        logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')
        protocol.send_ok(client_sock)

    def __handle_fin(self, client_sock):
        agency_id = protocol.recv_agency_id(client_sock)
        self._finished_agencies.add(agency_id)

        if len(self._finished_agencies) == self._agencies_amount:
            self._lottery_done = True
            logging.info("action: sorteo | result: success")

        protocol.send_ok(client_sock)

    def __handle_winners(self, client_sock):
        agency_id = protocol.recv_agency_id(client_sock)

        if not self._lottery_done:
            protocol.send_wait(client_sock)
            return

        winners = [b for b in load_bets() if b.agency == agency_id and has_won(b)]
        protocol.send_winners(client_sock, winners)

    def __handle_client_connection(self, client_sock):
        try:
            msg_type = protocol.recv_msg_type(client_sock)

            if msg_type == b'B':
                self.__handle_batch(client_sock)
            elif msg_type == b'F':
                self.__handle_fin(client_sock)
            elif msg_type == b'W':
                self.__handle_winners(client_sock)
        except OSError:
            pass
        except Exception as e:
            logging.error(
                f'action: apuesta_recibida | result: fail | cantidad: 0 | error: {e}'
            )
            try:
                protocol.send_error(client_sock)
            except Exception:
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
