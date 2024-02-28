import socket
import threading
#Redis class to handle the server, usign threading to handle multiple clients

class RedisClient:

    def __init__(self, socket, address):
       self.sock = socket
       self.address = address

    def recv(self):
        return self.sock.recv(1024)

    def send(self, data):
        self.sock.send(data.encode())
    
    def hc(self):
        while True:
            data = self.recv()
            print("->",data)
            if data == b"*1\r\n$4\r\nping\r\n":
                self.send("+PONG\r\n")
            if not data:
                break
        self.sock.close()