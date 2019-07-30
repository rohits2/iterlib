from threading import Thread
from multiprocessing import Process, Lock
from multiprocessing import Queue as PicklingQueue
from queue import Queue

from typing import Iterator

from loguru import logger

SENTINEL = "ITERLIB_PRELOADER_SENTINEL"

class Preloader:
    def __init__(self, input_iter: Iterator, buffer_size=4, verbose=False, mode="thread"):
        """
        Preloads an iterator in the background so that queries to it return quickly.
        Specify `max_buf` to control the maximum number of values to preload.
        Specify `mode` to control whether or not the background loading happens in 
        a thread or in a process.

        Be aware that if it happens in a process, objects must be pickable.
        If it occurs in a thread, the Python GIL will apply.
        """
        assert max_buf >= 1, "Buffer size must be greater than or equal to 1!"
        self.__in_iter = iter(input_iter)
        self.__done = False
        self.__read_lock = Lock()
        self.__verbose = verbose
        if mode == "thread":
            self.__out_queue = Queue(buffer_size)
            self.__worker = Thread(target=self.__work)
            self.__worker.start()
        elif mode == "process":
            self.__out_queue = PicklingQueue(buffer_size)
            self.__worker = Process(target=self.__work)
            self.__worker.start()
        else:
            raise ValueError("Expected either 'thread' or 'process' - you chose %s" % mode)

    @logger.catch
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
            if type(rv) == type(SENTINEL) and rv == SENTINEL:
                self.__done = True
                raise StopIteration()
            return rv

def thread_preload(itr, buffer_size=4):
    """
    Load an iterator in the background on a different thread.
    This is only useful when the iterator does mainly of IO operations (reading files/network calls)
    or lots of callouts to optimized C code (numpy, scipy, etc.).

    Otherwise, the GIL will prevent this from preloading anything at all.
    
    This is also not useful in high-throughput situations.

    Set max_buf to control how far in advance the iterator will preload.
    """
    return Preloader(itr, buffer_size=buffer_size, mode="thread")

def process_preload(itr, buffer_size=4):
    """
    Load an iterator in the background in a different process.

    Be aware that the iterator and items in it must be pickable.

    Set max_buf to control how far in advance the iterator will preload.
    """
    return Preloader(itr, buffer_size=buffer_size, mode="process")