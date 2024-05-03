from typing import Dict, List
from collections import deque
from datetime import datetime, timedelta
from app.utils import RESPParser, generate_alphanumeric_string
from app.db import MemoryStorage
import base64
import socket

class Vault:

    ACK = b"ACK"
    CAPABILITY = b"capa"
    CONFIG = b"config"
    ECHO = b"echo"
    GET = b"get"
    GETACK = b"GETACK"
    MASTER = "master"
    INFO = b"info"
    LISTENING_PORT = b"listening-port"
    SET = b"set"
    SLAVE = "slave"
    PING = b"ping"
    PX = b"px"
    PSYNC = b"PSYNC"
    REPLICATION = b"replication"
    RELP_CONF = b"REPLCONF"

    OK = b"+OK\r\n"

    LEN_CAPABILITY = 2
    LEN_CONFIG = 1
    LEN_ECHO = 2
    LEN_GET = 2
    LEN_INFO = 2
    LEN_LISTENING_PORT = 2
    LEN_REPL_CONF = 1
    LEN_SET = 3
    LEN_PING = 1
    LEN_PX = 2
    LEN_PSYNC = 3
    LEN_GETACK = 2

    def __init__(self, config):
        self.config = vars(config)
        if self.config["master_host"] is None and self.config["master_port"] is None:
            self.role = self.MASTER
        else:
            self.role = self.SLAVE
        self.master_replicaid = generate_alphanumeric_string() if self.role == self.MASTER else ''
        self.master_repliaoffset = 0
        self.memory = MemoryStorage()
        self.buffers = {}
        self.replica_present = False
        self.already_connected_master = False

    def set_memory(self, set_vals: List, data: Dict):
        lifetime = None
        for i, (key, value) in enumerate(set_vals):
            key = RESPParser.convert_to_string(key)
            value = RESPParser.convert_to_string(value)
            if Vault.PX in data:
                lifetime = (
                    datetime.now() + timedelta(milliseconds=RESPParser.convert_to_int(data[Vault.PX][i]))
                )
            self.memory.save(key, value, lifetime)
        return Vault.OK

    def get_memory(self, key):
        key = RESPParser.convert_to_string(key)
        value = self.memory.get(key)
        return value
    
    def parse_arguments(self, input: List) -> Dict:
        curr = 0
        result={}
        while curr<len(input):
            if input[curr].lower()==Vault.PING:
                result[Vault.PING]=None
                curr+=Vault.LEN_PING
            elif input[curr].lower()==Vault.ECHO:
                result[Vault.ECHO] = input[curr+1]
                curr+=Vault.LEN_ECHO
            elif input[curr].lower()==Vault.SET:
                result[Vault.SET] = result.get(Vault.SET,[]) + [[input[curr+1], input[curr+2]]]
                curr+=Vault.LEN_SET
            elif input[curr].lower()==Vault.GET:
                result[Vault.GET] = input[curr+1]
                curr+=Vault.LEN_GET
            elif input[curr].lower()==Vault.PX:
                result[Vault.PX] = result.get(Vault.PX,[]) + [input[curr+1]]
                curr+=Vault.LEN_PX
            elif input[curr].lower()==Vault.CONFIG:
                result[Vault.CONFIG] = {}
                curr+=Vault.LEN_CONFIG
                config_result = result[Vault.CONFIG]
                if input[curr].lower()==Vault.GET:
                    config_result[Vault.GET] = input[curr+1]
                    curr+=Vault.LEN_GET
            elif input[curr].lower()==Vault.INFO:
                result[Vault.INFO] = input[curr+1]
                curr+=Vault.LEN_INFO
            elif input[curr]==Vault.RELP_CONF:
                result[Vault.RELP_CONF] = {}
                repl_result = result[Vault.RELP_CONF]
                curr+=Vault.LEN_REPL_CONF
                if input[curr]==Vault.LISTENING_PORT:
                    repl_result[Vault.LISTENING_PORT] = input[curr+1]
                    curr+=Vault.LEN_LISTENING_PORT
                elif input[curr]==Vault.GETACK:
                    repl_result[Vault.GETACK] = input[curr+1]
                    curr+=Vault.LEN_GETACK
                while curr<len(input) and input[curr]==Vault.CAPABILITY:
                    repl_result[Vault.CAPABILITY] = repl_result.get(Vault.CAPABILITY,[])+[input[curr+1]]
                    curr+=Vault.LEN_CAPABILITY
            elif input[curr]==Vault.PSYNC:
                result[Vault.PSYNC] = input[curr+1:]
                curr+=Vault.LEN_PSYNC
            else:
                # print(f"Unknown command {input[curr]}")
                pass
        return result
        
    def get_config(self, key):
        return self.config.get(key)
    
    def get_info(self, key = None):
        response = '\n'.join([
            f"role:{self.role}",
            f"master_replid:{self.master_replicaid}",
            f"master_repl_offset:{self.master_repliaoffset}",
        ])
        return response 
    
    def do_handshake(self):
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((self.config["master_host"], self.config["master_port"]))

        client_sock.send(RESPParser.convert_list_to_resp(["ping"]))
        pong = client_sock.recv(1024)
        response = [Vault.RELP_CONF,"listening-port",self.config["port"]]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        response = [Vault.RELP_CONF, "capa", "eof", "capa", "psync2"]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        response = [Vault.PSYNC, "?","-1"]
        client_sock.send(RESPParser.convert_list_to_resp(response))
        pong = client_sock.recv(1024)
        rdb=client_sock.recv(1024)
    
        return client_sock

    def rdb_parsed(self):
        rdb = base64.b64decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
        length = len(rdb)
        parsed = (b"$" + RESPParser.convert_to_binary(length) + b"\r\n" + rdb)
        return parsed
        
    def is_master(self):
        return self.role == Vault.MASTER
        
    def add_command_buffer(self, command):
        for k,_ in self.buffers.items():
            self.buffers[k].append(command)
        return 0
        
    def add_new_replica(self):
        """
            This function takes care of everything needed to add a new replica
            1. Create a new buffer
            Returns the ID of the buffer to use
        """
        self.replica_present = True
        Id = len(self.buffers)
        self.buffers[Id] = deque([])
        return Id
