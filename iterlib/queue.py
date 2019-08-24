from threading import RLock, Event, Condition
from collections import deque

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
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self.queue = deque(maxlen=self.maxsize)
        self.error = None
        self.input_closed = False
        self.output_closed = False
        self.mutex = RLock()
        self.not_empty = Condition(self.mutex)
        self.not_full = Condition(self.mutex)
    
    @property
    def is_empty(self):
        with self.mutex:
            return len(self.queue) == 0

    @property
    def is_full(self):
        with self.mutex:
            return len(self.queue) == self.maxsize

    def put(self, item, block=True, timeout=None):
        with self.mutex:
            self.not_full.wait_for(lambda: (not self.is_full) or self.input_closed or self.output_closed, timeout=timeout)
            if self.input_closed:
                raise InputClosed()
            if self.output_closed:
                raise OutputClosed()
            if self.is_full and not block:
                return False
            self.queue.append(item)
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
            rv = self.queue.popleft()
            self.not_full.notify()
            return rv
            
    def set_error(self, exc):
        with self.mutex:
            self.error = exc
            self.input_closed = True
            self.output_closed = True
            self.queue.clear()
            self.not_empty.notify_all()
            self.not_full.notify_all()

    def close_input(self):
        with self.mutex:
            self.input_closed = True
            self.not_empty.notify_all()
            self.not_full.notify_all()

    def close_output(self):
        with self.mutex:
            self.output_closed = True
            self.not_empty.notify_all()
            self.not_full.notify_all()

    def __iter__(self):
        return QueueIterator(self)

    def __len__(self):
        with self.mutex:
            return len(self.queue)