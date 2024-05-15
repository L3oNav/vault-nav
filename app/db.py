from typing import Any
from datetime import datetime
import time

class Item:
    def __init__(self, value: str, lifetime: datetime):
        self.value = value
        self.lifetime = lifetime

class MemoryStorage:
    
    storage: dict[str, Item]

    def __init__(self):
        self.storage = {}

    def save(self, key, value, lifetime: datetime = None):
        self.storage[key] = Item(value, lifetime) 
    
    def get(self, key):
        item = self.storage.get(key)
        if item:
            if item.lifetime and self.expired(key, item.lifetime):
                del self.storage[key]
                return None
            return item.value
        return None

    def get_type(self, key):
        """
        Get the type of the value stored at key
        str: string
        """
        item = self.storage.get(key)
        _type = None
        if item:
            if item.lifetime and self.expired(key, item.lifetime):
                del self.storage[key]
                return None
            _type = type(item.value).__name__ 

        if _type is None:
            return 'none'
        elif _type == 'str':
            return 'string'
        elif _type == 'int':
            return 'integer'
        elif _type == 'float':
            return 'float'
                
    def delete(self, key):
        if key in self.storage:
            del self.storage[key]
            return 1
        return 0
    
    def expired(self, key, item_lifetime):
        if item_lifetime < datetime.now():
            self.save(key, None)
            return True
        return False
    
    def printall(self):
        # is storage emply
        
        if not self.storage:
            print("Memory is empty")
            return

        for key, item in self.storage.items():
            print(f"{key}: {item.value}")

    def exists(self, key):
        return key in self.storage
    
    def keys(self):
        return self.storage.keys()
    
    def flush(self):
        self.storage.clear()
    
