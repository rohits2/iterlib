import pytest
import iterlib
import random

def test_map_index_equality():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_sq = [x**2 for x in random_ints]
    random_ints_thread_map = iterlib.thread_map(lambda x: x**2, random_ints)
    random_ints_proc_map = iterlib.process_map(lambda x: x**2, random_ints)
    for i in range(10000):
        assert random_ints_sq[i] == random_ints_proc_map[i]
        assert random_ints_sq[i] == random_ints_thread_map[i]

def test_thread_map_order_equality():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_sq = [x**2 for x in random_ints]
    random_ints_thread_map = iterlib.thread_map(lambda x: x**2, random_ints, num_workers=128, verbose=True)
    for i, j in zip(random_ints_sq, random_ints_thread_map):
        assert i == j

def test_process_map_order_equality():
    random_ints = [random.randint(0, 100) for _ in range(10000)]
    random_ints_sq = [x**2 for x in random_ints]
    random_ints_proc_map = iterlib.process_map(lambda x: x**2, random_ints, num_workers=128, verbose=True)
    for i, j in zip(random_ints_sq, random_ints_proc_map):
        assert i == j
