# Copyright (c) 2025 iiPython

# Modules
import struct
import typing
import asyncio
from enum import Enum

import aiosqlite

# Initialization
class Response(Enum):
    HELO = 0
    OPOK = 1
    FAIL = 2

# Main object
class Carbon:
    def __init__(self) -> None:
        self.db: typing.Optional[aiosqlite.Connection] = None

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

        while read_stream:
            payload = await read_stream.read(30)
            if not payload:
                break

            # Begin reading data stream
            (
                transaction_id,
                transaction_type,
                key_length,
                value_length
            ) = struct.unpack(">21sBII", payload)

            # Logging
            print("=" * 50)
            print("Transaction type:", transaction_type)
            print("Key length:", key_length)
            print("Value length:", value_length)

            # Forcefully read the key
            key = await read_stream.read(key_length)
            print("Key:", key)

            # Handle transaction
            match transaction_type:
                case 0:  # PING
                    write_stream.write(self.build_response(Response.HELO))

                case 1:  # WRIT
                    value = (await read_stream.read(value_length)).decode("utf-8")
                    await self.db.execute("INSERT OR REPLACE INTO items VALUES (?, ?)", (key, value))
                    write_stream.write(self.build_response(Response.OPOK))

                case 2:  # READ
                    async with self.db.execute("SELECT value FROM items WHERE key = ?", (key,)) as cursor:
                        value = await cursor.fetchone() or ("null",)
                        write_stream.write(self.build_response(Response.OPOK, value[0]))

                case 3:  # WIPE
                    await self.db.execute("DELETE FROM items WHERE key = ?", (key,))
                    write_stream.write(self.build_response(Response.OPOK))

                case 4:  # AUTH
                    write_stream.write(self.build_response(Response.FAIL))

            await write_stream.drain()

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
