from typing import Iterator
from multiprocessing import cpu_count, Lock, Manager, Process, Value
from .queue import Queue, InputClosed, OutputClosed, QueueEmpty
from threading import Thread
from time import sleep

from loguru import logger

from .common import THREAD, PROCESS
from .allocator import StreamAllocator, ModulusAllocator


class IndexedMap:
    def __init__(self, func, *iters, mode=THREAD, num_workers=4, buffer_size=16, verbose=False):
        """
        Map `func` over `iters`, where each item in `iters` supports indexing.
        This map will also support indexing, and index queries will lazy evaluate the map.

        Calling `iter()` on this object will return an `IndexedStream` created with the remaining parameters.
        Subsequent calls to `iter()` **will return new `IndexedStream`s**.
        This behavior is substantially different than the builtin `map()`.
        """
        assert buffer_size >= 1, "Buffer size must be greater than or equal to 1!"
        assert num_workers >= 1, "Num workers must be greater than or equal to 1!"

        if mode not in [THREAD, PROCESS]:
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

class MapStream:
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

        self.__length = float('inf')
        self.__allocator = StreamAllocator(zip(*iters), num_workers)
        for it in iters:
            if not hasattr(it, "__len__"):
                self.__length = None
                break
            self.__length = min(self.__length, len(it))

        self.__control_lock = Lock()
        self.__running_workers = Value("i", num_workers+1)
        self.__i = Value("i", num_workers+1)

        if mode == THREAD:
            executor = Thread
        elif mode == PROCESS:
            executor = Process
        else:
            raise ValueError("Expected either iterlib.THREAD or iterlib.PROCESS")

        self.__output_queues = [Queue(1, mode=mode) for _ in range(num_workers)]
        self.__final_queue = Queue(buffer_size, mode=mode)

        self.__workers = []
        for i in range(num_workers):
            self.__workers += [executor(target=self.__work, args=(i,), daemon=True)]
            self.__workers[-1].start()

        self.__accumulator = Thread(target=self.__accumulate, daemon=True)
        self.__accumulator.start()

    def __work(self, mod):
        if self.__verbose:
            logger.debug("Worker %s is starting" % mod)

        in_iter = self.__allocator.get_iterator(mod)
        out_queue = self.__output_queues[mod]

        try:
            for x in in_iter:
                out_queue.put(self.__func(*x))
                if self.__verbose:
                    logger.debug("Worker %s completed one map" % mod)
            else:
                if self.__verbose:
                    logger.debug("Worker %s is shutting down without an error" % mod)
                
        except InputClosed:
            logger.debug("Worker %s is shutting down because the output queue is not accepting additional data" % mod)
        except Exception as ex:
            if self.__verbose:
                logger.warning("Detected error in worker %s: %s" % (mod, ex))
            out_queue.set_error(ex)
        finally:
            out_queue.close_input()
            with self.__control_lock:
                self.__running_workers.value -= 1
            if self.__verbose:
                logger.debug("Worker %s has shut down - there are now %s workers left" % (mod, self.__running_workers.value))
    
    def __accumulate(self):
        while not self.__final_queue.input_closed:
            queue_i = self.__i.value % self.__num_workers
            if self.__verbose:
                logger.debug("Requested item from subqueue %s which currently contains %s items" % (queue_i, len(self.__output_queues[queue_i])))
            try:
                rv = self.__output_queues[queue_i].get()
                self.__final_queue.put(rv)
                self.__i.value += 1
            except QueueEmpty:
                if self.__verbose:
                    logger.debug("Detected subqueue %s closure, ending iteration" % queue_i)
                self.__final_queue.close_input()
            except Exception as e:
                if self.__verbose:
                    logger.debug("Detected exception in subqueue %s, terminating workers" % queue_i)
                self.__final_queue.set_error(e)
                self.__final_queue.close_input()
                self.terminate()

        if self.__verbose:
            logger.debug("Accumulator shut down.")
        self.__final_queue.close_input()
        
        with self.__control_lock:
            self.__running_workers.value -= 1

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.__final_queue.get()
        except (OutputClosed, QueueEmpty):
            raise StopIteration()

    def terminate(self):
        if self.__verbose:
            logger.info("Terminating map!")
        for queue_i, worker in enumerate(self.__workers):
            self.__output_queues[queue_i].close_output()
            self.__output_queues[queue_i].close_input()
        for worker in self.__workers:
            if not worker.is_alive():
                continue
            worker.join()
    
    def is_shutdown(self):
        if self.__verbose:
            logger.info("There are %s workers running" % self.__running_workers.value)
        return self.__running_workers.value == 0

class IndexedMapStream(MapStream):
    def __init__(self, func, *iters, mode=THREAD, num_workers=4, buffer_size=16, verbose=False):
        super().__init__(func, *iters, mode=mode, num_workers=num_workers, buffer_size=buffer_size, verbose=verbose)
        if self._MapStream__length is None:
            raise ValueError("This map does not have a length!")
    
    def __len__(self):
        return self._MapStream__length


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
        return IndexedMap(func, *iters, mode=THREAD, num_workers=num_workers, buffer_size=16, verbose=verbose)
    else:
        return MapStream(func, *iters, mode=THREAD, num_workers=num_workers, buffer_size=buffer_size, verbose=verbose)


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
        return IndexedMap(func, *iters, mode=PROCESS, num_workers=num_workers, buffer_size=16, verbose=verbose)
    else:
        return MapStream(func, *iters, mode=PROCESS, num_workers=num_workers, buffer_size=buffer_size, verbose=verbose)