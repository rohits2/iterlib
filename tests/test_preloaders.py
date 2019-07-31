import pytest
import iterlib
import random

@pytest.mark.timeout(10)
def test_thread_loader_equality():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_sq = (x**2 for x in random_ints)
    random_ints_sq_list = [x**2 for x in random_ints]
    random_ints_thread_loader = iterlib.thread_preload(random_ints_sq)
    for i, v in enumerate(random_ints_thread_loader):
        assert random_ints_sq_list[i] == v


@pytest.mark.timeout(10)
def test_process_loader_equality():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_sq = (x**2 for x in random_ints)
    random_ints_sq_list = [x**2 for x in random_ints]
    random_ints_proc_loader = iterlib.process_preload(random_ints_sq)
    for i, v in enumerate(random_ints_proc_loader):
        assert random_ints_sq_list[i] == v
