import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


def flatten_list(input_list):
    return [x for xl in input_list for x in xl]

def generate_alphanumeric_string():
        import random
        import string
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(40))

class RESPParser:
    
    NULL_STRING = b"$-1\r\n"

    @staticmethod
    def process(string):
        """
        Function identified appropriate way to parse the information in RESP format
        """
        if string[0:1]==b"+":
            result = [RESPParser.process_simple_string(string)]
        elif string[0:1]==b"*":
            result = RESPParser.process_arrays(string)
        elif string[0:1]==b"$":
            result = [RESPParser.process_bulk_strings(string)]
        else:
            return string
        return result
    
    @staticmethod
    def process_arrays(input):
        input_list = input.split(b"*")[1:]
        split = lambda x: x.split(b"\r\n")[::2][1:] # get every other item
        input_list = flatten_list([split(x) for x in input_list])
        print(input_list)
        return input_list

    @staticmethod
    def process_bulk_strings(input):
        input_processed = input.split(b"\r\n")
        return input_processed[1]

    @staticmethod
    def process_simple_string(input):
        """
        Extracts the string item from the input
        """
        # +PONG\r\n
        input_processed = input[1:-2]
        return input_processed
    
    @staticmethod
    def convert_string_to_simple_string_resp(input):
        input_processed = b"+"+RESPParser.convert_to_binary(input)+\
            b"\r\n"
        return input_processed

    @staticmethod
    def convert_string_to_bulk_string_resp(input):
        length = RESPParser.convert_to_binary(len(input))
        input_processed = b"$"+length+b"\r\n"+\
            RESPParser.convert_to_binary(input)+b"\r\n"
        return input_processed

    @staticmethod
    def convert_list_to_resp(input):
        length = RESPParser.convert_to_binary(len(input))
        output = b"*"+length+b"\r\n"
        for item in input:
            item = RESPParser.convert_to_string(item)
            len_str = RESPParser.convert_to_binary(len(item))
            output += b"$"+len_str+b"\r\n"+\
                RESPParser.convert_to_binary(item)+\
                    b"\r\n"
        return output

    @staticmethod
    def convert_to_binary(input):
        if isinstance(input,bytes):
            return input
        elif isinstance(input,str):
            return input.encode("UTF-8")
        elif isinstance(input,int):
            return str(input).encode('UTF-8')
        else:
            raise ValueError(f"Expected input to be string, bytes or integer, \
                             but found {input} of type {type(input)}")

    @staticmethod
    def convert_to_string(input):
        if isinstance(input, str):
            return input
        elif isinstance(input,bytes):
            return input.decode('UTF-8')
        elif isinstance(input,int):
            return str(input)
        else:
            raise ValueError(f"Expected input to be string, bytes or integer, \
                             but found {input} of type {type(input)}")

    @staticmethod
    def convert_to_int(input):
        if isinstance(input, int):
            return input
        elif isinstance(input, str):
            return int(str)
        elif isinstance(input, bytes):
            return int(input)
        else:
            raise ValueError(f"Expected input to be int, bytes or integer, \
                             but found {input} of type {type(input)}")
