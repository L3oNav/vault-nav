import base64
import time

class Command(object):
    def __init__(self, data) -> None:
        self.cmd = None
        self.data = data
        self.type = None
        
    def __str__(self):
        pass

    def execute(self):
        pass

    def format(self):
        if type(self.data) == list:
            arr = self.data
        else:
            arr = self.data.split()
        res = []
        res.append("*{}".format(len(arr)))
        for i in arr:
            res.append("${}".format(len(i)))
            res.append(i)
        res_s = "\r\n".join(res) + "\r\n"
        print(res_s.encode())
        return res_s.encode()
    def get_type(self):
        return self.type