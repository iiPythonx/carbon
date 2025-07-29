# Copyright (c) 2025 iiPython

# Modules
import time
import json
import socket
import struct
import typing
import logging

from nanoid import generate
from carbon.enums import Transaction

# Exceptions
class NoAvailableNodes(Exception):
    pass

# Sandboxing
class Sandbox:
    """A sandbox representing the state of a collection in the database."""
    
    def __init__(self, database, name: str, initial_value: dict[str, typing.Any] = {}) -> None:
        self._db = database
        self._name: str = name
        self._value: dict[str, typing.Any] = initial_value

    def __getattribute__(self, name: str) -> typing.Any:
        if name in ["_db", "_name", "_value", "save"]:
            return object.__getattribute__(self, name)

        return self._value.get(name)

    def __setattr__(self, name: str, value: typing.Any) -> None:
        if name in ["_db", "_name", "_value"]:
            return object.__setattr__(self, name, value)

        self._value[name] = value

    def __delattr__(self, name: str) -> None:
        del self._value[name]

    def save(self) -> None:
        """Take all changes in this sandbox and propagate them to the database."""
        logging.debug(f"Sandbox '{self._name}' is now propagating changes to the database!")
        self._db.write(self._name, self._value)

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
        """Automatically selects a backend database based on its latency."""
        open_sockets = []
        for host in self.hosts:
            start = time.time()

            # Setup socket connection
            try:
                connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                connection.connect((host.split(":")[0], int(host.split(":")[1]) if ":" in host else 13051))

            except ConnectionError:
                logging.debug(f"[ACK] The specified host '{host}' is unreachable.")
                continue

            # Send ping and record time
            connection.sendall(self.build_transaction(Transaction.PING, "TIME"))
            if struct.unpack(">BI", connection.recv(5))[0] != 0:
                logging.debug(f"[ACK] The specified host '{host}' did not respond with HELO, skipping it.")
                continue

            open_sockets.append((connection, time.time() - start, host))
            logging.debug(f"[ACK] Host '{host}' is up and response latency was {round(open_sockets[-1][1] * 1000, 2)}ms.")

        if not open_sockets:
            raise NoAvailableNodes

        open_sockets.sort(key = lambda _: _[1])
        for connection, *_ in open_sockets[1:]:
            connection.close()  # Kill off the slower nodes

        connection, latency, current_host = open_sockets[0]
        logging.debug(f"[ACK] Host selected with latency {round(latency * 1000, 2)}ms.")

        self.active_connection = connection
        del open_sockets

        # Transmit the peer list
        if len(self.hosts) > 1:
            self.transact(Transaction.PEER, "LIST", "/".join(host for host in self.hosts if host != current_host))

    # Handle transactions
    @staticmethod
    def build_transaction(type: Transaction, key: str, value: typing.Optional[typing.Any] = None) -> bytes:
        """Build a transaction based on its type and associated data."""
        if not isinstance(value, bytes):
            value = json.dumps(value).encode("utf-8") if value is not None else b""

        return struct.pack(
            ">21sBBI",
            generate().encode("ascii"),
            type.value,
            len(key),
            len(value),
        ) + key.encode("ascii") + value

    def transact(self, type: Transaction, key: str, value: typing.Optional[typing.Any] = None) -> tuple[int, bytes]:
        """Build and transmit a transaction to the current backend database."""
        if self.active_connection is None:
            raise NoAvailableNodes

        packet = self.build_transaction(type, key, value)
        logging.debug(f"Sending packet to node: {packet}")
        self.active_connection.sendall(packet)

        # Receive result as well as the packet size
        result, packet_size = struct.unpack(">BI", self.active_connection.recv(5))
        return result, self.active_connection.recv(packet_size)

    def write(self, key: str, value: typing.Any) -> None:
        """Write the specified key and its associated value to the database. The value can be anything, as long as it is JSON serializable."""
        self.transact(Transaction.WRIT, key, value)

    def read(self, key: str) -> typing.Any:
        """Read the value of the specified key from the database."""
        return json.loads(self.transact(Transaction.READ, key)[1].decode("utf-8"))

    def delete(self, key: str) -> None:
        """Remove the specified key from the database."""
        self.transact(Transaction.WIPE, key)

    def auth(self, password: str) -> None:
        """Authenticate with the current backend database."""
        self.transact(Transaction.AUTH, "PSW", password)

    # Handle sandboxing
    def sandbox(self, collection: str) -> Sandbox:
        """Start a new sandbox, this is how you make bulk changes to the database."""
        return Sandbox(
            self,
            collection,
            self.read(collection) or {}
        )
