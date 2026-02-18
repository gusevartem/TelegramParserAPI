import asyncio
import sys

from parser.opentele.api import UseCurrentSession
from parser.opentele.td import TDesktop


async def main(tdata_folrder: str):
    tdesk = TDesktop(tdata_folrder)
    assert tdesk.isLoaded()
    client = await tdesk.ToTelethon(session="telethon.session", flag=UseCurrentSession)
    await client.connect()


def run():
    if len(sys.argv) != 2:
        raise ValueError("Usage: poetry run convert_to_telethon <tdata_folder>")
    asyncio.run(main(sys.argv[1]))
