import pytest
import iterlib
import random

@pytest.mark.timeout(10)
def test_thread_loader_cleanup():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_sq = (x**2 for x in random_ints)
    random_ints_sq_list = [x**2 for x in random_ints]
    random_ints_thread_loader = iterlib.thread_preload(random_ints_sq)
    for i, v in enumerate(random_ints_thread_loader):
        assert random_ints_sq_list[i] == v
    list(random_ints_thread_loader)
    assert random_ints_thread_loader.is_shutdown()

@pytest.mark.timeout(10)
def test_process_loader_cleanup():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_sq = (x**2 for x in random_ints)
    random_ints_proc_loader = iterlib.process_preload(random_ints_sq)
    list(random_ints_proc_loader)
    assert random_ints_proc_loader.is_shutdown()

@pytest.mark.timeout(10)
def test_thread_map_cleanup():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_thread_map = iter(iterlib.thread_map(lambda x: x**2, random_ints))
    list(random_ints_thread_map)
    assert random_ints_thread_map.is_shutdown()


@pytest.mark.timeout(10)
def test_process_map_cleanup():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_process_map = iter(iterlib.process_map(lambda x: x**2, random_ints))
    list(random_ints_process_map)
    assert random_ints_process_map.is_shutdown()

