import sys

SERVER_CONFIG = """name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    healthcheck:
      test: ["CMD", "python3", "-c", "import socket; s=socket.socket(); s.connect(('localhost',12345)); s.close()"]
      interval: 1s
      timeout: 3s
      retries: 10
      start_period: 2s
    volumes:
      - ./server/config.ini:/config.ini
"""

NETWORK_CONFIG = """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

def client_count(args):
    try:
        return int(args[2])
    except ValueError:
        print("Cantidad de clientes invalida, debe ser un entero.")
        sys.exit(1)

def define_client(client_id):
    return f"""  client{client_id}:
    container_name: client{client_id}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={client_id}
      - NOMBRE=Santiago Lionel
      - APELLIDO=Lorca
      - DOCUMENTO=30904465
      - NACIMIENTO=1999-03-17
      - NUMERO=7574
    networks:
      - testing_net
    depends_on:
      server:
        condition: service_healthy
    volumes:
      - ./client/config.yaml:/config.yaml
"""

def main():
    args = sys.argv
    if len(args) != 3:
        print("Uso: python3 mi-generador.py <archivo_salida> <cantidad_clientes>")
        sys.exit(1)

    output_file = args[1]
    clients = client_count(args)

    with open(output_file, "w") as f:
        f.write(SERVER_CONFIG)
        for client_id in range(1, clients + 1):
            f.write(define_client(client_id))
        f.write(NETWORK_CONFIG)

if __name__ == "__main__":
    main()