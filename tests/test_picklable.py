import pytest
import iterlib
import random

class Unpicklable:
    def __init__(self, q):
        self.func1 = lambda x: q*x**2
        self.func2 = lambda x: q*x
        self.q = q

@pytest.mark.timeout(10)
def test_thread_map_pickle():
    TEST_VAL = 5
    objs = [Unpicklable(random.randint(0, 1000)) for _ in range(100)]
    identity_map_objs = iterlib.thread_map(lambda x: x, objs)
    for orig, mapped in zip(objs, identity_map_objs):
        assert orig.q == mapped.q
        assert orig.func1(TEST_VAL) == mapped.func1(TEST_VAL)
        assert orig.func2(TEST_VAL) == mapped.func2(TEST_VAL)

@pytest.mark.timeout(10)
def test_thread_preload_pickle():
    TEST_VAL = 5
    objs = [Unpicklable(random.randint(0, 1000)) for _ in range(100)]
    identity_map_objs = iterlib.thread_preload(objs)
    for orig, mapped in zip(objs, identity_map_objs):
        assert orig.q == mapped.q
        assert orig.func1(TEST_VAL) == mapped.func1(TEST_VAL)
        assert orig.func2(TEST_VAL) == mapped.func2(TEST_VAL)

@pytest.mark.timeout(10)
def test_process_imap_pickle():
    objs = [random.randint(0, 1000) for _ in range(100)]
    unpicklable_objs_iterator = iterlib.thread_preload(objs)
    identity_map_objs = iterlib.process_map(lambda x: x, unpicklable_objs_iterator)
    for orig, mapped in zip(objs, identity_map_objs):
        assert orig == mapped