"""
Protocolo binario --> 4 bytes longitud + datos CSV
Formato CSV: agency,first_name,last_name,document,birthdate,number\n
"""


def recv_all(sock, n):
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise OSError("connection closed")
        data += chunk
    return data

# Formato: 4 bytes longitud + datos CSV
def recv_bet(sock):
    raw_len = recv_all(sock, 4)
    msg_len = int.from_bytes(raw_len, byteorder='big')
    raw_msg = recv_all(sock, msg_len)
    msg = raw_msg.decode('utf-8').strip()
    return msg if msg else None


def send_ok(sock):
    sock.send(b'OK\n')


def send_error(sock):
    sock.send(b'ERROR\n')
