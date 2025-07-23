# Copyright (c) 2025 iiPython

# Modules
import time
import json
import socket
import struct
import typing
import logging
from enum import Enum

from nanoid import generate

# Initialization
class Transaction(Enum):
    PING = 0
    WRIT = 1
    READ = 2
    WIPE = 3
    AUTH = 4

logging.basicConfig(level = logging.DEBUG)

# Exceptions
class NoAvailableNodes(Exception):
    pass

# Main object
class CarbonDB:
    def __init__(self, hosts: list[str], authentication: typing.Optional[str] = None) -> None:
        """Initialize a new Carbon session, with the given list of hosts and auth."""
        self.hosts = hosts
        self.authentication = authentication

        # Identify the lowest latency host
        self.active_connection: typing.Optional[socket.socket] = None
        self.select_host()

    # Handle host selection
    def select_host(self) -> None:
        open_sockets = []
        for host in self.hosts:
            start = time.time()

            # Setup socket connection
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.connect((host, 13051))

            # Send ping and record time
            connection.sendall(self.build_transaction(Transaction.PING, "TIME"))
            if connection.recv(4) != b"HELO":
                logging.debug(f"[ACK] The specified host '{host}' did not respond with HELO, skipping it.")
                continue

            open_sockets.append((connection, time.time() - start))
            logging.debug(f"[ACK] Host '{host}' is up and response latency was {round(open_sockets[-1][1] * 1000, 2)}ms.")

        if not open_sockets:
            raise NoAvailableNodes

        open_sockets.sort(key = lambda _: _[1])
        for connection, _ in open_sockets[1:]:
            print("killed", connection)
            connection.close()  # Kill off the slower nodes

        connection, latency = open_sockets[0]
        logging.debug(f"[ACK] Host selected with latency {round(latency * 1000, 2)}ms.")

        self.active_connection = connection
        del open_sockets

    # Handle transactions
    @staticmethod
    def build_transaction(type: Transaction, key: str, value: typing.Optional[typing.Any] = None) -> bytes:
        value = json.dumps(value).encode("utf-8") if value is not None else b""
        return struct.pack(
            ">21sBII",
            generate().encode("ascii"),
            type.value,
            len(key),
            len(value),
        ) + key.encode("ascii") + value

    def transact(self, type: Transaction, key: str, value: typing.Optional[typing.Any] = None) -> None:
        if self.active_connection is None:
            raise NoAvailableNodes

        packet = self.build_transaction(type, key, value)
        logging.debug(f"Sending packet to node: {packet}")
        self.active_connection.sendall(packet)
        print(self.active_connection.recv(4))

    def write(self, key: str, value: typing.Any) -> None:
        self.transact(Transaction.WRIT, key, value)

    def read(self, key: str) -> None:
        self.transact(Transaction.READ, key)
