import socket
import uuid
# slaves is a class to collect all the slaves connected to the master
class Slaves:

    def __init__(self):
        self.slaves: list[Slave] = []
    
    def add_slave(self, slave):
        self.slaves.append(slave)
    
    def get_slaves(self):
        return self.slaves

# slave is a class to handle the connection between the master and the slave
class Slave:
    
    def __init__(self, connection):
        self.connection = connection
        self._id: str = str(uuid.uuid4())