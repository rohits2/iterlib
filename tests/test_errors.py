import pytest
import iterlib
import random
from time import sleep
from utils import wait_until_shutdown

@pytest.mark.timeout(10)
def test_thread_loader_exception():
    random_ints = [1 for _ in range(1000)] + [0] + [1 for _ in range(1000)]
    random_ints_inv = (1.0/x for x in random_ints)
    random_ints_thread_loader = iterlib.thread_preload(random_ints_inv)
    with pytest.raises(ZeroDivisionError):
        list(random_ints_thread_loader)
    assert wait_until_shutdown(random_ints_thread_loader)

@pytest.mark.timeout(10)
def test_process_loader_exception():
    random_ints = [1 for _ in range(1000)] + [0] + [1 for _ in range(1000)]
    random_ints_inv = (1.0/x for x in random_ints)
    random_ints_proc_loader = iterlib.process_preload(random_ints_inv)
    with pytest.raises(ZeroDivisionError):
        inv_ints = list(random_ints_proc_loader)
    assert wait_until_shutdown(random_ints_proc_loader)

@pytest.mark.timeout(10)
def test_thread_map_exception():
    random_ints = [1 for _ in range(1000)] + [0] + [1 for _ in range(1000)]
    random_ints_inv = iterlib.thread_map(lambda x: 1.0/x, random_ints, verbose=True)
    random_ints_thread_loader = iter(random_ints_inv)
    with pytest.raises(ZeroDivisionError):
        inv_ints = list(random_ints_thread_loader)
    assert wait_until_shutdown(random_ints_thread_loader)

@pytest.mark.timeout(10)
def test_process_map_exception():
    random_ints = [1 for _ in range(1000)] + [0] + [1 for _ in range(1000)]
    random_ints_inv = iterlib.process_map(lambda x: 1.0/x, random_ints)
    random_ints_proc_loader = iter(random_ints_inv)
    with pytest.raises(Exception):
        inv_ints = list(random_ints_proc_loader)
    assert wait_until_shutdown(random_ints_proc_loader)
