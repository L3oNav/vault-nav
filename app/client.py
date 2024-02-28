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