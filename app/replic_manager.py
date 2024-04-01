import socket
import asyncio

class ReplicationClient:

    def __init__(self, host, port, current_port, replicaid, replicaoffset) -> None:
        self.host = host
        self.port = int(port)
        self.current_port = int(current_port)
        self.master = socket.socket()
        self.master.connect((self.host, self.port))
        self.replicaid = replicaid
        self.replicaoffset = replicaoffset
        self.role = "slave"

    def handshake(self):
        try:
            self.master.send("*1\r\n$4\r\nping\r\n".encode())
            print("master answer: ", self.master.recv(1024).decode())
            self.master.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{self.current_port}\r\n".encode())
            print("master answer: ", self.master.recv(1024).decode())
            self.master.sendall("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode())
            print("master answer: ", self.master.recv(1024).decode())
            self.master.sendall("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())
        except BrokenPipeError:
            self.master = socket.socket()
            self.master.connect((self.host, self.port))
            self.handshake()

    def connect(self, skt, address):
        try:
            while True:
                data = skt.recv(1024)
                if not data:
                    break
                print("data sended: ", data)
                self.master.send(data)
        except ConnectionResetError or BrokenPipeError:
            print("Connection error, trying to reconnect...")
            self.master = socket.socket()
            self.master.connect((self.host, self.port))
            self.connect(skt, address)