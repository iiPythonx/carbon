# Copyright (c) 2025 iiPython

# Modules
import struct
import asyncio

# Main object
class Carbon:
    def __init__(self) -> None:
        pass

    async def handle(self, read_stream: asyncio.StreamReader, write_stream: asyncio.StreamWriter) -> None:
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

            # Handle transaction
            match transaction_type:
                case 0:  # PING
                    write_stream.write(b"HELO")

                case 1:  # WRIT
                    write_stream.write(b"OK")

                case 2:  # READ
                    write_stream.write(b"OK")

                case 3:  # WIPE
                    write_stream.write(b"OK")

                case 4:  # AUTH
                    write_stream.write(b"OK")

            await write_stream.drain()

            # Logging
            print("Transaction type:", transaction_type)
            print("Key length:", key_length)
            print("Value length:", value_length)
            print("Key:", await read_stream.read(key_length))
            print("Value:", await read_stream.read(value_length))
            print("=" * 50)

        # Kill the stream
        write_stream.close()
        await write_stream.wait_closed()

# Main launching
async def main() -> None:
    async with await asyncio.start_server(Carbon().handle, "0.0.0.0", 13051, keep_alive = True) as host:
        await host.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
