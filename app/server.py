import socket
from app.client import RedisClient


class RedisServer:
    def __init__(self, host="localhost", port=6379):
        server_socket = socket.create_server((host, port), reuse_port=True)
        skt, skt_addr = server_socket.accept()
        self.client = RedisClient(skt, skt_addr)
    
    def run(self):
        while True:
            res = self.client.recv()
            if res == b"*1\r\n$4\r\nping\r\n":
                self.client.send("$4\r\nPONG\r\n")
            elif res == b"":
                break
            