from threading import Thread
from app.vault import Vault
from app.utils import RESPParser
import socket
import asyncio

class ServerMaster(Thread):

    def __init__(self, conn, vault: Vault, do_handshake: bool = False):
        super().__init__()
        self.vault = vault
        self.conn = conn
        self.talking_to_master = do_handshake
        self.talking_to_replica = False
        self.buffer_id = None

    def run(self):
        print("Server running")
        while True:
            if self.talking_to_replica:
                break

            original_message = self.conn.recv(1024)

            if not original_message:
                break

            print(f"Original message: {original_message}, Data pre-process: {original_message}")
            data = RESPParser.process(original_message)
            data = self.vault.parse_arguments(data)

            if Vault.PING in data:
                self.conn.send(RESPParser.convert_string_to_simple_string_resp(b"PONG"))

            elif Vault.ECHO in data:
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp(data[Vault.ECHO]))

            elif Vault.SET in data:
                print(f"setting {data[Vault.SET]}, {data}")
                self.vault.set_memory(data[Vault.SET], data)
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
                conf = data[Vault.RELP_CONF]
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))

            elif Vault.PSYNC in data:
                self.conn.send(
                    RESPParser.convert_string_to_simple_string_resp(
                        f"FULLRESYNC {self.vault.master_replicaid} {self.vault.master_repliaoffset}"
                    )
                )
                response = self.vault.rdb_parsed()
                self.talking_to_replica=True
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
        while True:
            thread_queue = self.vault.buffers[self.buffer_id]
            if len(thread_queue)>0:
                command = thread_queue.popleft()
                print("Sending command to replica: ", command)
                self.conn.send(command)
                print(thread_queue)


class ServerSlave(Thread):

    def __init__(self, vault: Vault):
        super().__init__()
        self.vault = vault 
        self.conn = self.vault.do_handshake()

    def run(self):
        while True and self.conn is not None:
            original_message = self.conn.recv(1024)

            if not original_message:
                break

            print(f"Original message: {original_message}, Data pre-process: {original_message}")
            data = RESPParser.process(original_message)
            data = self.vault.parse_arguments(data)

            if Vault.SET in data:
                print(f"setting {data[Vault.SET]}, {data}")
                self.vault.set_memory(data[Vault.SET],data)
                # self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
            elif Vault.RELP_CONF in data and Vault.GETACK in data[Vault.RELP_CONF]:
                self.conn.send(
                    RESPParser.convert_list_to_resp([Vault.RELP_CONF, Vault.ACK, "0"])
                )
            else:
                self.conn.send(b"-Error message\r\n")
            if self.vault.replica_present and Vault.SET in data:
                print("Adding SET command to buffer")
                self.vault.add_command_buffer(original_message)
        self.conn.close()
