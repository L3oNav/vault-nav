import socket
import threading
from datetime import datetime, timedelta
from app.db import MemoryStorage
import base64
#Redis class to handle the server, usign threading to handle multiple clients

class RedisClient:

    def __init__(self, sock, address, port,role: str = 'master', replicaid: str = '', replicaoffset: int = 0, master_host: str = None, master_port: int = None):
        self.storage = MemoryStorage()
        self.sock = sock
        self.address = address
        self.port = port
        self.role = role
        self.replicaid = replicaid
        self.replicaoffset = replicaoffset
        if self.role == "slave":
            self.master_host = str(master_host)
            self.master_port = int(master_port)
            self.master = socket.socket()
            self.master.connect((self.master_host, self.master_port))
    
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
        data = self.sock.recv(1024).decode()
        print("request: ",data)
        return data
    
    def send(self, data):
        self.sock.sendall(data.encode("utf-8"))

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
                print("INFO command received")
                response = '\n'.join([
                    f"role:{self.role}",
                    f"master_replid:{self.replicaid}",
                    f"master_repl_offset:{self.replicaoffset}",
                ])
                self.send(f"${len(response)}\r\n{response}\r\n")
            #replicaof
            if cmmd == "REPLICAOF" and args:
                host = args[0]
                port = args[1]
                self.send("+OK\r\n")
            #psync
            if cmmd == "PSYNC" and args:
                response = (f"+FULLRESYNC {self.replicaid} {self.replicaoffset}\r\n")
                self.send(f"${len(response)}\r\n{response}\r\n")
                rdb = base64.b64decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
                self.sock.send(f"${len(rdb)}\r\n".encode() + rdb)
            #replconf
            if cmmd == "REPLCONF" and args:
                self.send("+OK\r\n")
            #command not found
            if not cmmd:
                print("command not found")
                break
        self.sock.close()