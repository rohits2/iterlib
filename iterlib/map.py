from threading import Thread
from time import sleep
from types import MethodType
from typing import Iterator
from multiprocessing import cpu_count, Lock, Queue, Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from loguru import logger

TERMINATION_OBJECT = {"END NOW"}


class StreamingMap:
    def __init__(self, func, *iters, executor=ThreadPoolExecutor, num_workers=4, buffer_size=16, verbose=False):
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"

        self.__func = func
        self.__iters = iters
        self.__length = min((len(x) for x in iters))
        self.__verbose = verbose

        self.__num_workers = num_workers
        self.__buffer_size = buffer_size

        self.__workers = executor(max_workers=num_workers)
        self.__worker_queues = [Queue(buffer_size)]
        for i in range(num_workers):
            self.__workers.submit(self.__work, i, self.__worker_queues[i])

        self.__read_lock = Lock()
        self.__done = False
        self.__i = 0

        self.__workers.shutdown(wait=False)

    def __work(self, mod, queue):
        def source_iter():
            for i in range(mod, self.__length, self.__num_workers):
                yield tuple([it[i] for it in self.__iters])

        f = self.__func[0]
        for x in source_iter():
            queue.put(f(x))
            if self.__verbose:
                logger.debug("Worker %s completed one map" % mod)
        queue.put(TERMINATION_OBJECT)

    def __len__(self):
        return self.__length

    def __iter__(self):
        return self

    def __next__(self):
        with self.__read_lock:
            if self.__done:
                raise StopIteration()
            queue_i = self.__i % self.__num_workers
            if self.__verbose:
                logger.debug("Requested item from subqueue %s which currently contains %s items" % (queue_i, self.__worker_queues[queue_i].qsize()))
            rv = self.__worker_queues[queue_i].get()
            if rv is TERMINATION_OBJECT:
                logger.debug("Detected termination object in subqueue %s, ending iteration")
                self.__done = True
                raise StopIteration()
            self.__i += 1
            return rv


class IndexedMap:
    def __init__(self, func, *iters, executor=ThreadPoolExecutor, num_workers=4, buffer_size=16):
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"
        
        self.__func = [func]
        self.__iters = iters
        self.__length = min((len(x) for x in iters))

        self.__num_workers = num_workers
        self.__buffer_size = buffer_size

        self.__executor = executor

    def __len__(self):
        return self.__length

    def __getitem__(self, i):
        return tuple([it[i] for it in self.__iters])

    def __iter__(self):
        return StreamingMap(self.__func[0], *self.__iters, executor=self.__executor, num_workers=self.__num_workers, buffer_size=self.__buffer_size)


def thread_map(func, *iters, num_workers=4, buffer_size=16):
    return IndexedMap(func, iters, executor=ThreadPoolExecutor, num_workers=num_workers, buffer_size=16)

def process_map(func, *iters, num_workers=4, buffer_size=16):
    return IndexedMap(func, iters, executor=ProcessPoolExecutor, num_workers=num_workers, buffer_size=16)