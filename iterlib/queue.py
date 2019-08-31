from multiprocessing import RLock, Event, Condition, Value
from queue import Queue as TQueue
from multiprocessing import Queue as MQueue

from .common import drain_queue, THREAD, PROCESS


class InputClosed(Exception):
    def __init__(self):
        super().__init__()

class OutputClosed(Exception):
    def __init__(self):
        super().__init__()

class QueueEmpty(Exception):
    def __init__(self):
            super().__init__()

class QueueIterator:
    def __init__(self, queue):
        self.queue = queue
    def __iter__(self):
        return self
    def __next__(self):
        try:
            return self.queue.get()
        except (OutputClosed, QueueEmpty):
            raise StopIteration

class Queue:
    def __init__(self, maxsize=0, mode=THREAD):
        self.maxsize = maxsize
        self.mode = mode

        self.__input_closed = Value('i', 0)
        self.__output_closed = Value('i', 0)
        self.__has_error = Value('i', 0)
        self.mutex = RLock()
        self.not_empty = Condition(self.mutex)
        self.not_full = Condition(self.mutex)

        if mode == THREAD:
            self.queue = TQueue(maxsize=maxsize)
            self.__error_queue = TQueue(maxsize=maxsize)
        else:
            self.queue = MQueue(maxsize=maxsize)
            self.__error_queue = MQueue(maxsize=maxsize)
            
    
    @property
    def is_empty(self):
        with self.mutex:
            return self.queue.empty()

    @property
    def is_full(self):
        with self.mutex:
            return self.queue.full()

    @property
    def input_closed(self):
        with self.mutex:
            return self.__input_closed.value

    @property
    def output_closed(self):
        with self.mutex:
            return self.__input_closed.value

    @property
    def error(self):
        with self.mutex:
            if not self.__has_error.value:
                return None
            ev = self.__error_queue.get()
            self.__error_queue.put(ev)
            return ev

    def put(self, item, block=True, timeout=None):
        with self.mutex:
            self.not_full.wait_for(lambda: (not self.is_full) or self.input_closed or self.output_closed, timeout=timeout)
            if self.input_closed:
                raise InputClosed()
            if self.output_closed:
                raise OutputClosed()
            if self.is_full and not block:
                return False
            self.queue.put(item)
            self.not_empty.notify()
        return True

    def get(self, block=True, timeout=None):
        with self.mutex:
            self.not_empty.wait_for(lambda: (not self.is_empty) or self.input_closed or self.output_closed, timeout=timeout)
            if self.error is not None:
                raise self.error
            if self.is_empty and (not block or self.input_closed):
                raise QueueEmpty()
            if self.output_closed:
                raise OutputClosed()
            rv = self.queue.get()
            self.not_full.notify()
            return rv
            
    def set_error(self, exc):
        with self.mutex:
            self.__error_queue.put(exc)
            self.__has_error.value = 1
            self.__input_closed.value= 1
            self.__output_closed.value = 1
            drain_queue(self.queue)
            self.not_empty.notify_all()
            self.not_full.notify_all()

    def close_input(self):
        with self.mutex:
            self.__input_closed.value = 1
            self.not_empty.notify_all()
            self.not_full.notify_all()

    def close_output(self):
        with self.mutex:
            self.__output_closed.value = 1
            self.not_empty.notify_all()
            self.not_full.notify_all()

    def __iter__(self):
        return QueueIterator(self)

    def __len__(self):
        with self.mutex:
            return self.queue.qsize()

