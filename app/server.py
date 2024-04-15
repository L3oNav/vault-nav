from threading import Thread
from app.vault import Vault
from app.utils import RESPParser
import socket
import asyncio

class Server(Thread):
    def __init__(self, conn, vault: Vault, do_handshake: bool = False):
        super().__init__()
        self.vault = vault
        self.conn = conn
        self.talking_to_master = do_handshake
        self.talking_to_replica = False
        self.buffer_id = None

    def run(self):
        while True:
            if self.talking_to_replica:
                break
            original_message = self.conn.recv(1024)
            # print(original_message)
            if not original_message:
                break
            data = RESPParser.process(original_message)
            data = self.vault.parse_arguments(data)
            if Vault.PING in data:
                self.conn.send(RESPParser.convert_string_to_simple_string_resp(b"PONG"))
            elif Vault.ECHO in data:
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp(data[Vault.ECHO]))
            elif Vault.SET in data:
                self.vault.set_memory(data[Vault.SET],data)
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
            elif Vault.GET in data:
                result = self.vault.get_memory(data[Vault.GET])
                if result is None:
                    result = RESPParser.NULL_STRING
                    self.conn.send(result)
                else:
                    self.conn.send(RESPParser.convert_string_to_bulk_string_resp(result))
            elif Vault.CONFIG in data:
                config_data = data[Vault.CONFIG]
                if Vault.GET in config_data:
                    result = self.vault.get_config(config_data[Vault.GET])
                if result is None:
                    result = RESPParser.NULL_STRING
                    self.conn.send(result)
                else:
                    self.conn.send(RESPParser.convert_list_to_resp([config_data[Vault.GET],result]))
            elif Vault.INFO in data:
                info = self.vault.get_info()
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp(info))
            elif Vault.RELP_CONF in data:
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
            elif Vault.PSYNC in data:
                self.conn.send(RESPParser.convert_string_to_simple_string_resp(f"FULLRESYNC {self.vault.master_replicaid} {self.vault.master_repliaoffset}"))

                response = self.vault.rdb_parsed()
                self.talking_to_replica=True # if the code reaches here, that means it is talking to the replica
                self.buffer_id = self.vault.add_new_replica()
                self.conn.send(response)
            else:
                self.conn.send(b"-Error message\r\n")
            if self.vault.replica_present and Vault.SET in data:
                self.vault.add_command_buffer(original_message)
        if self.talking_to_replica and self.vault.is_master():
            print(f"Connected to replica {self.buffer_id}")
            self.run_sync_replica()
        self.conn.close()

    def run_sync_replica(self):
        """
        This function checks if there is any new information in the queue and sends it to the replica server
        """
        while True:
            thread_queue = self.vault.buffers[self.buffer_id]
            if len(thread_queue)>0:
                command = thread_queue.popleft()
                print(f"sending {command}")
                self.conn.send(command)
                print(thread_queue)
                # _ = self.conn.recv(1024)


class ServerMasterConnectThread(Thread):
    def __init__(self, vault: Vault):
        super().__init__()
        self.vault = vault 
        self.conn = None

    def run(self):
        self.conn = self.vault.do_handshake()
        while True:
            original_message = self.conn.recv(1024)
            print(original_message)
            if not original_message:
                break
            data = RESPParser.process(original_message)
            data = self.vault.parse_arguments(data)
            if Vault.PING in data:
                pass
            elif Vault.SET in data:
                print(f"setting {data[Vault.SET]}")
                self.vault.set_memory(data[Vault.SET],data)
                # self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
            elif Vault.RELP_CONF in data:
                # If slave connected to master receives it, return this value
                self.conn.send(RESPParser.convert_list_to_resp([Vault.RELP_CONF,Vault.ACK,
                                                                self.vault.master_repl_offset]))
            else:
                self.conn.send(b"-Error message\r\n")
            if self.vault.replica_present and Vault.SET in data:
                self.vault.add_command_buffer(original_message)
        print("Closing Replica connection")
        self.conn.close()