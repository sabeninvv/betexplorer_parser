from src import *


@external_sentinel
def run_simplenote_parser(**kwargs):
    SimplenoteParser(**kwargs).filling_request()


@external_sentinel
def run_handler(**kwargs):
    be_parser = BetExplorerParser(aiohttp_session=None, telegram_cli=None, **kwargs)
    be_parser.cold_start()
    be_parser.handler()


@external_sentinel
def run_betexplorer_parser(**kwargs):
    with asyncio.Runner() as runner:
        aiohttp_session = create_aiohttp_session(loop=runner.get_loop())
        telegram_cli = TelegramClient(session=aiohttp_session)
        be_parser = BetExplorerParser(
            aiohttp_session=aiohttp_session,
            telegram_cli=telegram_cli,
            **kwargs)

        runner.run(be_parser.ride_round_robin())


if __name__ == "__main__":
    conn_read, conn_write = Pipe(duplex=False)
    container = {
        'module_name': "Entry",
        'logger': get_logger("main"),
        'conn_read': conn_read,
        'conn_write': conn_write,
    }

    processes = [
        Process(target=run_simplenote_parser, kwargs=container),
        Process(target=run_handler, kwargs=container),
        Process(target=run_betexplorer_parser, kwargs=container)
    ]

    start_processes(processes=processes, **container)
