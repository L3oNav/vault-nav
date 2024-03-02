from typing import Any


class MemoryStorage:

    def __init__(self):
        self.storage = {}

    def set(self, key, value):
        self.storage[key] = value 
    
    def get(self, key):
        return self.storage.get(key)
    
    def delete(self, key):
        if key in self.storage:
            del self.storage[key]
            return 1
        return 0
    
    def exists(self, key):
        return key in self.storage
    
    def keys(self):
        return self.storage.keys()
    
    def flush(self):
        self.storage.clear()
    