import sys
from pathlib import Path

from alembic.config import main as alembic_main


def run() -> None:
    ini_path = Path(__file__).resolve().parent / "alembic.ini"
    args = sys.argv[1:]
    sys.argv = ["alembic", "-c", str(ini_path), *args]
    alembic_main()
