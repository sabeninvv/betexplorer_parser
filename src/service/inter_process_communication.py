from typing import Union, List, Optional
from time import sleep
from multiprocessing import Queue, Process
from multiprocessing.managers import SyncManager, DictProxy
from threading import Thread
from multiprocessing.connection import wait, Connection
from logging import Logger
import signal
import os


def term_process(pid: Optional[int] = None):
    pid = os.getpid() if pid is None else pid
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def term_shared_dict(val: DictProxy):
    val['terminate'] = True
    val.clear()


def terminate(processes: List[Union[Process, Thread]], **kwargs):
    kwargs['metrics_queue'].put({'readiness': False, 'liveness': False})
    _ = [term_process(pid=process.pid) for process in processes if isinstance(process, Process)]
    _ = [term_shared_dict(val) for _, val in kwargs.items() if isinstance(val, DictProxy)]
    _ = [val.shutdown() for _, val in kwargs.items() if isinstance(val, SyncManager)]
    sleep(10)
    term_process()


def start_processes(processes: List[Union[Process, Thread]], **kwargs):
    logger: Logger = kwargs['logger']
    module_name: str = kwargs['module_name']
    conn_read: Connection = kwargs['conn_read']

    for process in processes:
        process.daemon = True
        process.start()

    module_start_progress = {
        'total': int(len(processes) * 10),
        'fraction': int(int(len(processes) * 10) // len(processes)),
        'now': 0
    }
    objs4monitoring = [getattr(p, 'sentinel', None) for p in processes]
    objs4monitoring = [sentinel for sentinel in objs4monitoring if sentinel is not None] + [conn_read]
    while True:
        sentinels = wait(objs4monitoring, timeout=10.)
        if not sentinels:
            continue
        if any(
                (isinstance(sentinel, int) for sentinel in sentinels)
        ):
            terminate(processes, **kwargs)
            return

        msg = conn_read.recv()
        if msg == "liveness":
            module_start_progress['now'] += module_start_progress['fraction']
            if module_start_progress['now'] >= module_start_progress['total']:
                logger.info(f"[{module_name}] started OK {kwargs.get('start_msg', '')}")
        elif isinstance(msg, Exception):
            logger.error(msg, exc_info=True)
            terminate(processes, **kwargs)
            return

