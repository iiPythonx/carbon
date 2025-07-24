# Copyright (c) 2025 iiPython

# Modules
import time
import shutil
import struct
import typing
import asyncio
from datetime import datetime

import aiosqlite

from carbon import __version__
from carbon.typing import Response, Transaction

# Handle logging
class Logging:
    def __init__(self) -> None:
        print("\033[2J\033[H", end = "")
        self.log("!", f"Carbon v{__version__} is running, Copyright (c) 2025 iiPython.")

    def log(self, icon: str, text: str) -> None:
        timestamp = datetime.now().strftime("%D %I:%M:%S %p")
        print(f"\033[1;30;41m {icon} \033[0;39;40m {text}{' ' * (shutil.get_terminal_size()[0] - (len(text) + len(timestamp) + 7))} \033[1;30;44m {timestamp} \033[0m")

    def add_transaction(self, type: int, response: bytes, start_time: float, transaction_id: str) -> None:
        response_type, packet_size = response[0], str(int.from_bytes(response[1:5]))
        self.log("@", f"Code: {type} ({Transaction(type).name}) │ Response: {response_type} ({Response(response_type).name}) │ Size: {packet_size.zfill(5)}b │ Transaction: {transaction_id} │ Elapsed: {round((time.perf_counter_ns() - start_time) / 1000, 2)}μs")

    def add_connection(self, type: str, source: str) -> None:
        self.log("!", f"Connection {type} from {source[0]}:{source[1]}.")
        
# Main object
class Carbon:
    def __init__(self) -> None:
        self.db: typing.Optional[aiosqlite.Connection] = None
        self.logging: Logging = Logging()

    @staticmethod
    def build_response(result: Response, data: str = "") -> bytes:
        return struct.pack(
            ">BI",
            result.value,
            len(data)
        ) + data.encode("ascii")

    async def init(self) -> None:
        self.db = await aiosqlite.connect("carbon.db")
        await self.db.execute("""\
            CREATE TABLE IF NOT EXISTS items (
                key   STRING,
                value STRING
            )
        """)

    async def handle(self, read_stream: asyncio.StreamReader, write_stream: asyncio.StreamWriter) -> None:
        if self.db is None:
            raise RuntimeError("The database is not connected internally, this is a bad sign.")

        address = write_stream.get_extra_info("peername")
        self.logging.add_connection("established", address)

        while read_stream:
            payload = await read_stream.read(30)
            if not payload:
                break

            start_time = time.perf_counter_ns()

            # Begin reading data stream
            (
                transaction_id,
                transaction_type,
                key_length,
                value_length
            ) = struct.unpack(">21sBII", payload)

            key = (await read_stream.read(key_length)).decode("utf-8")
            value = (await read_stream.read(value_length)).decode("utf-8")

            # Handle transaction
            match transaction_type:
                case 0:  # PING
                    response = self.build_response(Response.HELO)

                case 1:  # WRIT
                    await self.db.execute("INSERT OR REPLACE INTO items VALUES (?, ?)", (key, value))
                    response = self.build_response(Response.OPOK)

                case 2:  # READ
                    async with self.db.execute("SELECT value FROM items WHERE key = ?", (key,)) as cursor:
                        value = await cursor.fetchone() or ("null",)

                    response = self.build_response(Response.OPOK, value[0])

                case 3:  # WIPE
                    await self.db.execute("DELETE FROM items WHERE key = ?", (key,))
                    response = self.build_response(Response.OPOK)

                case 4:  # AUTH
                    response = self.build_response(Response.FAIL, "Authentication not supported on this database.")

                case _:
                    response = self.build_response(Response.FAIL, "The specified transaction type does not exist.")

            self.logging.add_transaction(transaction_type, response, start_time, transaction_id.decode())

            write_stream.write(response)
            await write_stream.drain()

        self.logging.add_connection("lost", address)

        # Kill the stream
        write_stream.close()
        await write_stream.wait_closed()

# Main launching
async def main() -> None:
    carbon = Carbon()
    await carbon.init()

    # Launch socket
    async with await asyncio.start_server(carbon.handle, "0.0.0.0", 13051, keep_alive = True) as host:
        await host.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
