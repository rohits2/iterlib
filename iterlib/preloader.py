from threading import Thread
from multiprocessing import Process, Queue, Lock

from typing import Iterator

SENTINEL = "ITERLIB_PRELOADER_SENTINEL"

class Preloader:
    def __init__(self, input_iter: Iterator, max_buf=4, verbose=False, mode="thread"):
        assert max_buf >= 1, "Buffer size must be greater than or equal to 1!"
        self.__in_iter = iter(input_iter)
        self.__out_queue = Queue(max_buf)
        self.__done = False
        self.__read_lock = Lock()
        self.__verbose = verbose
        if mode == "thread":
            self.__worker = Thread(target=self.__work)
            self.__worker.start()
        elif mode == "process":
            self.__worker = Process(target=self.__work)
            self.__worker.start()

    def __work(self):
        for item in self.__in_iter:
            self.__out_queue.put(item)
        self.__out_queue.put(SENTINEL)

    def __iter__(self):
        return self

    def __next__(self):
        with self.__read_lock:
            if self.__done:
                raise StopIteration()
            rv = self.__out_queue.get()
            if rv == SENTINEL:
                self.__done = True
                raise StopIteration()
            return rv

def thread_preload(itr, max_buf=4):
    """
    Load an iterator in the background on a different thread.
    This is only useful when the iterator does mainly of IO operations (reading files/network calls)
    or lots of callouts to optimized C code (numpy, scipy, etc.).

    Otherwise, the GIL will prevent this from preloading anything at all.
    
    This is also not useful in high-throughput situations.

    Set max_buf to control how far in advance the iterator will preload.
    """
    return Preloader(itr, max_buf=max_buf, mode="thread")

def process_preload(itr, max_buf=4):
    """
    Load an iterator in the background in a different process.

    Set max_buf to control how far in advance the iterator will preload.
    """
    return Preloader(itr, max_buf=max_buf, mode="process")