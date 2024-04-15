import socket
from datetime import datetime, timedelta
from app.db import MemoryStorage
from app.replicas import Slaves, Slave
import base64
#Redis class to handle the server, usign threading to handle multiple clients

class RedisClient:

    def __init__(self, sock, address, port, master, role: str = 'master', replicaid: str = '', replicaoffset: int = 0):
        self.storage = MemoryStorage()
        self.slaves = Slaves()
        self.sock = sock
        self.address = address
        self.port = port
        self.role = role
        self.replicaid = replicaid
        self.replicaoffset = replicaoffset
        self.master = master

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

    def encode_resp(self, command, arguments):
        response = f"*{len(arguments) + 1}\r\n${len(command)}\r\n{command}\r\n"
        for arg in arguments:
            response += f"${len(arg)}\r\n{arg}\r\n"
        return response


    def recv(self):
        data = self.sock.recv(1024).decode()
        return data

    def send(self, data, send_to_master=False):
        if send_to_master and not self.role == "master":
            self.master.send(data.encode())
        else:
            self.sock.send(data.encode())

    def commands_handler(self, cmmd, args) -> None:
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
            print(f"Command: SET {key} {value}, Require propagation to slaves: {self.slaves.get_slaves()}")
            if self.role == "master" and self.slaves.get_slaves():
                for conn in self.slaves.get_slaves():
                    print(f"Sending to slave {conn}")
                    conn.send(self.encode_resp(cmmd, args)) 
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
            self.slaves.add_slave(Slave(self.sock))
        #replconf
        if cmmd == "REPLCONF" and args:
            self.send("+OK\r\n")

    def hc(self):
        while True:
            data = self.recv()
            if not data:
                break
            command, arguments = self.parse_resp_command(data)
            if self.role == "master" or command in ["INFO"]:
               self.commands_handler(command, arguments) 
        self.sock.close()