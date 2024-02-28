import socket
import threading
#Redis class to handle the server, usign threading to handle multiple clients

class RedisClient:

    def __init__(self, socket, address):
       self.sock = socket
       self.address = address

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
            if cmmd == "PING":
                self.send("+PONG\r\n")
            #echo
            if cmmd == "ECHO" and args:
                message = args[0]
                response = f"${len(message)}\r\n{message}\r\n"
                self.send(response)
            if not cmmd:
                break
        self.sock.close()