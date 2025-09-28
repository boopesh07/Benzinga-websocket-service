import asyncio
from app.ws_client import main_async


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

