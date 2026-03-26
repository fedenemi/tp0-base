"""
Módulo de protocolo de comunicación entre cliente y servidor.

Protocolo binario. Formato de batch:
  4 bytes cantidad + [4 bytes longitud + datos CSV] * cantidad

Formato CSV: agency,first_name,last_name,document,birthdate,number\n
"""


def recv_all(sock, n):
    """Lee exactamente n bytes del socket. Lanza OSError si la conexión se cierra."""
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise OSError("connection closed")
        data += chunk
    return data


def recv_batch(sock):
    """
    Lee un batch de apuestas del socket.
    Formato: 4 bytes cantidad + [4 bytes longitud + datos CSV] * cantidad
    Retorna lista de strings CSV.
    """
    raw_count = recv_all(sock, 4)
    batch_count = int.from_bytes(raw_count, byteorder='big')

    messages = []
    for _ in range(batch_count):
        raw_len = recv_all(sock, 4)
        msg_len = int.from_bytes(raw_len, byteorder='big')
        raw_msg = recv_all(sock, msg_len)
        messages.append(raw_msg.decode('utf-8').strip())
    return messages


def send_ok(sock):
    """Envía confirmación de éxito."""
    sock.send(b'OK\n')


def send_error(sock):
    """Envía señal de error."""
    sock.send(b'ERROR\n')
