from typing import Iterator
from multiprocessing import cpu_count, Lock, Queue, Manager, Process
from threading import Thread
from loguru import logger

SENTINEL = "ITERLIB_STREAMING_MAP_SENTINEL"


class StreamingMap:
    def __init__(self, func, *iters, executor=Thread, num_workers=4, buffer_size=16, verbose=False):
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"

        self.__func = func
        self.__iters = iters
        self.__length = min((len(x) for x in iters))
        self.__verbose = verbose

        self.__num_workers = num_workers
        self.__buffer_size = buffer_size

        self.__worker_queues = [Queue(buffer_size) for _ in range(num_workers)]
        self.__workers = []
        for i in range(num_workers):
            self.__workers += [executor(target=self.__work, args=(i,))]
            self.__workers[-1].start()

        self.__read_lock = Lock()
        self.__done = False
        self.__i = 0


    def __work(self, mod):
        if self.__verbose:
            logger.debug("Worker %s is starting" % mod)

        queue = self.__worker_queues[mod]

        def source_iter():
            for i in range(mod, self.__length, self.__num_workers):
                yield [it[i] for it in self.__iters]

        for x in source_iter():
            queue.put(self.__func(*x))
            if self.__verbose:
                logger.debug("Worker %s completed one map" % mod)

        if self.__verbose:
            logger.debug("Worker %s is shutting down" % mod)
        queue.put(SENTINEL)

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
            if rv == SENTINEL:
                logger.debug("Detected termination object in subqueue %s, ending iteration")
                self.__done = True
                raise StopIteration()
            self.__i += 1
            return rv


class IndexedMap:
    def __init__(self, func, *iters, executor=Thread, num_workers=4, buffer_size=16, verbose=False):
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"

        self.__func = func
        self.__iters = iters
        self.__length = min((len(x) for x in iters))

        self.__num_workers = num_workers
        self.__buffer_size = buffer_size

        self.__executor = executor
        self.__verbose = verbose

    def __len__(self):
        return self.__length

    def __getitem__(self, i):
        return self.__func(*[it[i] for it in self.__iters])

    def __iter__(self):
        return StreamingMap(
            self.__func,
            *self.__iters,
            executor=self.__executor,
            num_workers=self.__num_workers,
            buffer_size=self.__buffer_size,
            verbose=self.__verbose)


def thread_map(func, *iters, num_workers=4, buffer_size=16, verbose=False):
    return IndexedMap(func, *iters, executor=Thread, num_workers=num_workers, buffer_size=16, verbose=verbose)


def process_map(func, *iters, num_workers=4, buffer_size=16, verbose=False):
    return IndexedMap(func, *iters, executor=Process, num_workers=num_workers, buffer_size=16, verbose=verbose)