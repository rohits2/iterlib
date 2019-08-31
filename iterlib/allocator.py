from threading import RLock, Event, Condition

from .common import drain_queue, THREAD, PROCESS

class StreamAllocator:
    def __init__(self, input_stream, num_clients, mode=THREAD):
        self.__input_stream = iter(input_stream)
        self.__sync_lock = RLock()

    def get_iterator(self, i):
        def stream_alloc_iter():
            with self.__sync_lock:
                yield next(self.__input_stream)
        return stream_alloc_iter()

class ModulusAllocator:
    def __init__(self, input_collection, num_clients, mode=THREAD):
        if not hasattr(input_collection, "__getitem__") or not hasattr(input_collection, "__len__"):
            raise TypeError("Input collection is not subscriptable!")
        self.__input_collection = input_collection
        self.__num_clients = num_clients
        self.__client_suballocators = []
        for i in range(self.__num_clients):
            self.__client_suballocators += [self.__make_iterator(0)]
        self.__global_lock = RLock()

    def __make_iterator(self, i):
        lock = RLock()
        def modulus_alloc_iter():
            nonlocal lock, self
            indices = range(0, len(self.__input_collection), self.__num_clients)
            for i in indices:
                yield self.__input_collection[i]
                
    def get_iterator(self, i):
        with self.__global_lock:
            return self.__input_collection[i]