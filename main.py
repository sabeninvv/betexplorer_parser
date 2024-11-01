from src import *


if os_name == 'nt':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@external_sentinel
def run_simplenote_parser(**kwargs):
    logger = kwargs['logger']
    SimplenoteParser().filling_request()


@external_sentinel
def run_handler(**kwargs):
    logger = kwargs['logger']
    while True:
        try:
            be_parser = BetExplorerParser()
            be_parser.cold_start()
            be_parser.handler()
        except Exception as ex:
            logger.error(f"[run_handler] {ex}")


@external_sentinel
def run_betexplorer_parser(**kwargs):
    logger = kwargs['logger']
    while True:
        try:
            asyncio.run(
                BetExplorerParser().run_event()
            )
        except Exception as ex:
            logger.error(f"[run_betexplorer_parser] {ex}")


if __name__ == "__main__":
    conn_read, conn_write = Pipe(duplex=False)
    container = {
        'module_name': "Entry",
        'logger': get_logger("main"),
        'conn_read': conn_read,
        'conn_write': conn_write,
    }
    logger = container['logger']

    processes = [
        Process(target=run_simplenote_parser, kwargs=container),
        Process(target=run_handler, kwargs=container),
        Process(target=run_betexplorer_parser, kwargs=container)
    ]

    start_processes(processes=processes, **container)
