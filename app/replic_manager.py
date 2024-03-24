import socket
import asyncio

class ReplicationClient:

    def __init__(self, host, port, current_port) -> None:
        self.host = host
        self.port = int(port)
        self.current_port = int(current_port)

    def connect(self):
        self.master = socket.socket()
        self.master.connect((self.host, self.port))
        data = "*1\r\n$4\r\nping\r\n"
        self.master.send(data.encode())
        response = self.master.recv(1024)
        self.master.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{self.current_port}\r\n".encode())
        self.master.sendall("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
        self.master.sendall("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())