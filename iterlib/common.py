from multiprocessing.queues import Empty as PicklingQueueEmpty
from multiprocessing.queues import Full as PicklingQueueFull
from queue import Empty, Full

MAP_SENTINEL = "ITERLIB_STREAMING_MAP_SENTINEL"
STREAMING_SENTINEL = "ITERLIB_PRELOADER_SENTINEL"
ERROR_SENTINEL = "ITERLIB_ERROR_SENTINEL"
DUMMY_VALUE = "ITERLIB_DUMMY_VALUE"

class IterlibException(Exception):
    def __init__(self, exceptions, *args, **kwargs):
        super().__init__(args, kwargs)
        self.exceptions = exceptions

def drain_queue(queue):
    try:
        while True:
            queue.get_nowait()
    except PicklingQueueEmpty:
        return
    except Empty:
        return

def fill_queue(queue, val):
    try:
        while True:
            queue.put_nowait(val)
    except PicklingQueueFull:
        return
    except Full:
        return