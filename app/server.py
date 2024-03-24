import socket
import asyncio
from app.client import RedisClient
from app.replic_manager import ReplicationClient
from threading import Thread

class Server(Thread):
    def __init__(self, config):
        self.server_socket = socket.create_server(("localhost", config.port))
        self.role = config.role
        self.master_host = config.master_host
        self.master_port = config.master_port
        self.replicaid = config.replicaid
        self.replicaoffset = config.replicaoffset
        if not self.is_master():
            self.master_client = ReplicationClient(self.master_host, self.master_port, current_port=config.port) 

    def is_master(self):
        return self.role == "master"

    def run(self):
        if not self.is_master():
                master = self.master_client
                master.connect()
        while True:
            skt, addr = self.server_socket.accept()
            self.client_sk = RedisClient(skt, addr, self.role, self.replicaid, self.replicaoffset)
            Thread(target=self.client_sk.hc, daemon=True).start()
