import socket
import asyncio

class ReplicationClient:

    def __init__(self, host, port) -> None:
        self.host = host
        self.port = int(port)

    def connect(self):
        self.master = socket.socket()
        self.master.connect((self.host, self.port))
        data = "*1\r\n$4\r\nping\r\n"
        self.master.send(data.encode())
        self.master.close()