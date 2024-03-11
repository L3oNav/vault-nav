# Uncomment this to pass the first stage
import socket
import asyncio
import argparse
from app.server import Server
from app.replic_manager import ReplicationClient
import hashlib


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
        self.master_host = args.replicaof[0] if args.replicaof else None
        self.master_port = args.replicaof[1] if args.replicaof else None
        self.role = "slave" if args.replicaof else "master"
        self.replicaid = generate_alphanumeric_string() if self.role == "master" else ''
        self.replicaoffset = 0

        @property
        def host(self):
            return self.host
        
        @property
        def port(self):
            return self.port
        
        @property
        def master_host(self):
            return self.master_host
        
        @property
        def master_port(self):
            return self.master_port

config = Conf(args)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    if config.port is None:
        print("Missing port argument")
    if config.role == "slave" and (config.master_host is None or config.master_port is None):
        print("Missing master_host or master_port argument")
    if config.role == "master" and (config.master_host is not None or config.master_port is not None):
        print("Master cannot have master_host or master_port arguments")
        
    server = Server(config)
    server.run()
   


if __name__ == "__main__":
    main() 
