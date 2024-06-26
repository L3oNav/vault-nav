import socket
import asyncio
import argparse
from app.server import ServerMaster, ServerSlave
from app.vault import Vault
import hashlib
from app.utils import logger

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="localhost", type=str, help="Host to bind the server")
parser.add_argument("--port", default=6379 ,type=int, help="Port to bind the server")
parser.add_argument("--replicaof", nargs=2, metavar=("master_host", "master_port") )

args = parser.parse_args()

def generate_alphanumeric_string():
        import random
        import string
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(40))

class Conf:
    def __init__(self, args):
        self.host = args.host
        self.port = args.port
        self.role = "slave" if args.replicaof else "master"
        self.replicaid = generate_alphanumeric_string() if self.role == "master" else ''
        self.replicaoffset = 0
        self.master_host = args.replicaof[0] if args.replicaof else None
        self.master_port = int(args.replicaof[1]) if args.replicaof else None

config = Conf(args)

def main():

    if config.port is None:
        logger.error("Missing port argument")
    if config.role == "slave" and (config.master_host is None or config.master_port is None):
        logger.error("Missing master_host or master_port argument")
    if config.role == "master" and (config.master_host is not None or config.master_port is not None):
        logger.error("Master cannot have master_host or master_port arguments")

    vault = Vault(config)
    local_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    local_socket.bind(("localhost", config.port))
    local_socket.listen()
    logger.info(f"Server started on {config.host}:{config.port}")

    if vault.role == Vault.SLAVE:
        logger.info(f"Slave from {config.master_host}:{config.master_port}")
        server = ServerSlave(vault) 
        server.start()

    while True:
        logger.info("Master waiting for connection")
        conn, addr = local_socket.accept()
        server = ServerMaster(conn, vault)
        server.start()

if __name__ == "__main__":
    logger.info("Starting server")
    logger.info(f"Role: {config.role}")
    logger.debug(f"{[config.__dict__]}")
    main()
