from typing import Iterator
from multiprocessing import cpu_count, Lock, Manager, Process
from multiprocessing import Queue as PicklingQueue
from queue import Queue
from threading import Thread

from loguru import logger

SENTINEL = "ITERLIB_STREAMING_MAP_SENTINEL"


class IndexedMapStream:
    def __init__(self, func, *iters, mode="thread", num_workers=4, buffer_size=16, verbose=False):
        """
        A parallel map operating on indexable objects.
        Specify `mode` to determine whether the map will use processes or threads.

        This will start `num_workers` workers and use the object's __getitem__ to distribute work.

        Every other worker will store `buffer_size` completed results.  
        Once they have that many completed results, they will stall until data is pulled from this object.

        Specifying `verbose=True` will print potentially useful timing and diagnostic information.
        """
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"

        self.__func = func
        self.__iters = iters
        self.__length = min((len(x) for x in iters))
        self.__verbose = verbose

        self.__num_workers = num_workers
        self.__buffer_size = buffer_size
        if mode == "thread":
            executor = Thread
            queue = Queue
        elif mode == "process":
            executor = Process
            queue = PicklingQueue
        else:
            raise ValueError("Expected either 'thread' or 'process' - you chose %s" % mode)

        self.__worker_queues = [queue(buffer_size) for _ in range(num_workers)]
        self.__workers = []
        for i in range(num_workers):
            self.__workers += [executor(target=self.__work, args=(i,))]
            self.__workers[-1].start()

        self.__read_lock = Lock()
        self.__done = False
        self.__i = 0

    @logger.catch
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
    
    def __del__(self):
        for worker in self.__workers:
            worker.terminate()
            worker.join()

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
            if type(rv) == type(SENTINEL) and rv == SENTINEL:
                logger.debug("Detected termination object in subqueue %s, ending iteration")
                self.__done = True
                raise StopIteration()
            self.__i += 1
            return rv


class IndexedMap:
    def __init__(self, func, *iters, mode="thread", num_workers=4, buffer_size=16, verbose=False):
        """
        Map `func` over `iters`, where each item in `iters` supports indexing.
        This map will also support indexing, and index queries will lazy evaluate the map.

        Calling `iter()` on this object will return an `IndexedStream` created with the remaining parameters.
        Subsequent calls to `iter()` **will return new `IndexedStream`s**.
        This behavior is substantially different than the builtin `map()`.
        """
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"

        if mode not in ["thread", "process"]:
            raise ValueError("Expected either 'thread' or 'process' - you chose %s" % mode)


        self.__func = func
        self.__iters = iters
        self.__length = min((len(x) for x in iters))

        self.__num_workers = num_workers
        self.__buffer_size = buffer_size

        self.__mode = mode
        self.__verbose = verbose

    def __len__(self):
        return self.__length

    def __getitem__(self, i):
        return self.__func(*[it[i] for it in self.__iters])

    def __iter__(self):
        return IndexedMapStream(
            self.__func,
            *self.__iters,
            mode=self.__mode,
            num_workers=self.__num_workers,
            buffer_size=self.__buffer_size,
            verbose=self.__verbose)

class GeneratorMapStream:
    def __init__(self, func, *iters, mode="thread", num_workers=4, buffer_size=16, verbose=False):
        """
        A parallel map operating on non-indexable generators.
        Specify `mode` to determine whether the map will use processes or threads.

        This will start `num_workers` workers of the specified type, plus 1 thread which is necessary to distribute work because the iterables
        may not be indexable.

        Every other worker will store `buffer_size` completed results.  
        Once they have that many completed results, they will stall until data is pulled from this object.

        Specifying `verbose=True` will print potentially useful timing and diagnostic information.
        """
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"

        self.__func = func
        self.__iters = iters
        self.__verbose = verbose

        self.__num_workers = num_workers
        self.__buffer_size = buffer_size

        if mode == "thread":
            executor = Thread
            queue = Queue
        elif mode == "process":
            executor = Process
            queue = PicklingQueue
        else:
            raise ValueError("Expected either 'thread' or 'process' - you chose %s" % mode)

        self.__input_queues = [queue(buffer_size) for _ in range(num_workers)]
        self.__worker_queues = [queue(buffer_size) for _ in range(num_workers)]
        self.__workers = []
        for i in range(num_workers):
            self.__workers += [executor(target=self.__work, args=(i,))]
            self.__workers[-1].start()

        self.__distributor = Thread(target=self.__distribute)
        self.__distributor.start()

        self.__read_lock = Lock()
        self.__done = False
        self.__i = 0

    @logger.catch
    def __work(self, mod):
        if self.__verbose:
            logger.debug("Worker %s is starting" % mod)

        in_queue = self.__input_queues[mod]
        out_queue = self.__worker_queues[mod]

        for x in iter(in_queue.get, SENTINEL):
            out_queue.put(self.__func(*x))
            if self.__verbose:
                logger.debug("Worker %s completed one map" % mod)

        if self.__verbose:
            logger.debug("Worker %s is shutting down" % mod)
        out_queue.put(SENTINEL)
    
    @logger.catch
    def __distribute(self):
        for i, vals in enumerate(zip(*self.__iters)):
            queue_i = i % self.__num_workers
            self.__input_queues[queue_i].put(vals)
        for i in range(self.__num_workers):
            self.__input_queues[queue_i].put(SENTINEL)
            self.__input_queues[queue_i].put(SENTINEL)

    def __del__(self):
        for worker in self.__workers:
            worker.terminate()
            worker.join()

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
            if type(rv) == type(SENTINEL) and rv == SENTINEL:
                logger.debug("Detected termination object in subqueue %s, ending iteration")
                self.__done = True
                raise StopIteration()
            self.__i += 1
            return rv



def thread_map(func, *iters, num_workers=2, buffer_size=3, verbose=False):
    """
    Map `func` over `iters` using `num_workers` threads.
    Specify `buffer_size` to control how much data each worker will store before waiting for values to be consumed from the map.
    If `verbose` is True, the map will print diagnostic information.

    Because this function uses threads, the function and iterables do NOT need to be picklable.
    However, the GIL will still prevent python code from running in parallel.
    """
    use_indexing = True
    for itr in iters:
        use_indexing = use_indexing and hasattr(itr, "__getitem__")

    if use_indexing:
        return IndexedMap(func, *iters, mode="thread", num_workers=num_workers, buffer_size=16, verbose=verbose)
    else:
        return GeneratorMapStream(func, *iters, mode="thread", num_workers=num_workers, buffer_size=buffer_size, verbose=verbose)


def process_map(func, *iters, num_workers=4, buffer_size=1, verbose=False):
    """
    Map `func` over `iters` using `num_workers` processes.
    Specify `buffer_size` to control how much data each worker will store before waiting for values to be consumed from the map.
    If `verbose` is True, the map will print diagnostic information.

    Because this map uses multiprocessing, both the function and the iterables must be picklable.
    """
    use_indexing = True
    for itr in iters:
        use_indexing = use_indexing and hasattr(itr, "__getitem__")

    if use_indexing:
        return IndexedMap(func, *iters, mode="process", num_workers=num_workers, buffer_size=16, verbose=verbose)
    else:
        return GeneratorMapStream(func, *iters, mode="process", num_workers=num_workers, buffer_size=buffer_size, verbose=verbose)