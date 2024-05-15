from threading import Thread
from app.vault import Vault
from app.utils import RESPParser, logger
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
        logger.info("Master running...")
        while True:
            if self.talking_to_replica:
                logger.info("Talking to replica")
                break

            original_message = self.conn.recv(1024)
            logger.debug(f"Master original message: {original_message}")
            if not original_message:
                logger.debug("No message received")
                break

            data = RESPParser.process(original_message)
            logger.debug(f"Master data post-process: {data}")
            data = self.vault.parse_arguments(data)
            logger.debug(f"Master data post-argument parsing: {data}")

            if Vault.PING in data:
                self.conn.send(RESPParser.convert_string_to_simple_string_resp(b"PONG"))

            elif Vault.ECHO in data:
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp(data[Vault.ECHO]))

            elif Vault.SET in data:
                self.vault.set_memory(data[Vault.SET], data)
                self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))

            elif Vault.GET in data:
                result = self.vault.get_memory(data[Vault.GET])
                if result is None:
                    result = RESPParser.NULL_STRING
                    self.conn.send(result)
                else:
                    self.conn.send(RESPParser.convert_string_to_bulk_string_resp(result))
            elif Vault.TYPE in data:
                result = self.vault.get_type(data[Vault.TYPE])
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
        logger.debug(f"Slave connection: {self.conn}")

    def run(self):
        logger.info("Slave running...")
        while True:
            original_message = self.conn.recv(1024)
            logger.info(f"Slave original message: {original_message}")

            if not original_message:
                logger.debug("No message received")
                break

            logger.debug(f"Data pre-process: {original_message}")
            data = RESPParser.process(original_message)
            logger.debug(f"Data post-process: {data}")
            data = self.vault.parse_arguments(data)
            logger.debug(f"Data post-argument parsing: {data}")

            if Vault.SET in data:
                logger.debug(f"Slave SET | {data[Vault.SET]}")
                self.vault.set_memory(data[Vault.SET],data)
                # self.conn.send(RESPParser.convert_string_to_bulk_string_resp("OK"))
            elif Vault.RELP_CONF in data:
                logger.debug(f"Slave REPLCONF | {data[Vault.RELP_CONF]}")
                self.conn.send(RESPParser.convert_list_to_resp([Vault.RELP_CONF, Vault.ACK, '0']))
            else:
                logger.debug("Error message")
                self.conn.send(b"-Error message\r\n")
            if self.vault.replica_present and Vault.SET in data:
                logger.debug(f"Adding SET | {data} | command to buffer")
                self.vault.add_command_buffer(original_message)
        logger.info("Slave connection closed")
        self.conn.close()
