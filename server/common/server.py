import socket
import logging
import signal
from common.utils import Bet, store_bets
from common import protocol


class Server:
    def __init__(self, port, listen_backlog):
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
        """Recibe un batch de apuestas, las persiste y responde OK."""
        try:
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
        except OSError:
            pass
        except Exception as e:
            logging.error(f'action: apuesta_recibida | result: fail | cantidad: 0 | error: {e}')
            try:
                protocol.send_error(client_sock)
            except Exception:
                pass
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
