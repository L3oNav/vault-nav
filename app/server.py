import socket
import argparse
from app.client import RedisClient
from threading import Thread

class RedisServer(Thread):
    def __init__(self, config):
        self.server_socket = socket.create_server(("localhost", config.port))
        self.role = config.role
        self.master_host = config.master_host
        self.master_port = config.master_port
        self.client_sk = None

    def run(self):
        while True:
            skt, addr = self.server_socket.accept()
            self.client_sk = RedisClient(skt, addr, self.role)
            Thread(target=self.client_sk.hc, daemon=True).start()