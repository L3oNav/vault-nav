# Uncomment this to pass the first stage
import socket
from app.server import RedisServer

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = RedisServer()
    server.run()


if __name__ == "__main__":
    main()
