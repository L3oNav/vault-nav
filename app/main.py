# Uncomment this to pass the first stage
import socket
import argparse
from app.server import RedisServer

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="localhost", type=str, help="Host to bind the server")
parser.add_argument("--port", default=6379, type=int, help="Port to bind the server")
args = parser.parse_args()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = RedisServer(host=args.host, port=args.port)
    server.run()


if __name__ == "__main__":
    main()
