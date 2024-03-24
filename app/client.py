import socket
import threading
from datetime import datetime, timedelta
from app.db import MemoryStorage
#Redis class to handle the server, usign threading to handle multiple clients

class RedisClient:

    def __init__(self, socket, address, role: str = 'master', replicaid: str = '', replicaoffset: int = 0):
        self.storage = MemoryStorage()
        self.sock = socket
        self.address = address
        self.role = role
        self.replicaid = replicaid
        self.replicaoffset = replicaoffset

    def parse_resp_command(self, data):
        lines = data.split("\r\n")
        command = None
        arguments = []
        if lines[0].startswith("*"):
            num_elements = int(lines[0][1:])
            for i in range(1, num_elements * 2, 2):
                if lines[i].startswith("$"):
                    arguments.append(lines[i + 1])
            if arguments:
                command = arguments[0].upper()
                arguments = arguments[1:]
        return command, arguments

    def recv(self):
        return self.sock.recv(1024).decode()

    def send(self, data):
        self.sock.send(data.encode())
    
    def hc(self):
        while True:
            cmmd, args = self.parse_resp_command(self.recv()) 
            #ping
            if cmmd == "PING":
                self.send("+PONG\r\n")
            #echo
            if cmmd == "ECHO" and args:
                message = args[0]
                response = f"${len(message)}\r\n{message}\r\n"
                self.send(response)
            #set, with expiration PX, px and pX are all valid
            if cmmd == "SET" and args:
                key = args[0]
                value = args[1]
                lifetime = None 
                if len(args) > 2:
                    if args[2].upper() == "PX" or args[2].upper() == "EX":
                        lifetime = datetime.now() + timedelta(
                            milliseconds=int(args[3])
                        )
                self.storage.set(key, value, lifetime)
                self.send("+OK\r\n")
            #get
            if cmmd == "GET" and args:
                key = args[0]
                value = self.storage.get(key)
                if value:
                    response = f"${len(value)}\r\n{value}\r\n"
                    self.send(response)
                else:
                    self.send("$-1\r\n")
            #info
            if cmmd == "INFO":
                response = '\n'.join([
                    f"role:{self.role}",
                    f"master_replid:{self.replicaid}",
                    f"master_repl_offset:{self.replicaoffset}",
                ])
                self.send(f"${len(response)}\r\n{response}\r\n")
            if cmmd == "REPLCONF" and args:
                host = args[0]
                port = args[1]
                self.send("+OK\r\n")
            if not cmmd:
                break
        self.sock.close()