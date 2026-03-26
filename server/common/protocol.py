"""

Protocolo binario --> Cada mensaje comienza con 1 byte de tipo:
  'B' - Batch de apuestas
  'F' - Fin de apuestas de una agencia
  'W' - Consulta de ganadores

-  4 bytes big-endian.
- cada apuesta: agency,first_name,last_name,document,birthdate,number\\n (CSV)
"""


def recv_all(sock, n):
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise OSError("connection closed")
        data += chunk
    return data


def recv_msg_type(sock):
    return recv_all(sock, 1)

# Formato: 4 bytes cantidad + [4 bytes longitud + datos CSV] * cantidad
#Retorna lista de strings CSV.
def recv_batch(sock):    
    raw_count = recv_all(sock, 4)
    batch_count = int.from_bytes(raw_count, byteorder='big')

    messages = []
    for _ in range(batch_count):
        raw_len = recv_all(sock, 4)
        msg_len = int.from_bytes(raw_len, byteorder='big')
        raw_msg = recv_all(sock, msg_len)
        messages.append(raw_msg.decode('utf-8').strip())
    return messages

def recv_agency_id(sock):
    raw = recv_all(sock, 4)
    return int.from_bytes(raw, byteorder='big')


def send_ok(sock):
    sock.send(b'OK\n')


def send_wait(sock):
    sock.send(b'WAIT\n')


def send_error(sock):
    sock.send(b'ERROR\n')


# Formato: 4 bytes cantidad + [4 bytes longitud + DNI bytes] * cantidad
def send_winners(sock, winners):
    sock.send(len(winners).to_bytes(4, byteorder='big'))
    for w in winners:
        dni_bytes = w.document.encode('utf-8')
        sock.send(len(dni_bytes).to_bytes(4, byteorder='big'))
        sock.send(dni_bytes)
