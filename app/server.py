import socket
from app.client import RedisClient
from threading import Thread

class RedisServer(Thread):
    def __init__(self, host="localhost", port=6379):
        self.server_socket = socket.create_server((host, port), reuse_port=True)

    def run(self):
        while True:
            skt, addr = self.server_socket.accept()
            self.client_sk = RedisClient(skt, addr)
            Thread(target=self.client_sk.hc).start()