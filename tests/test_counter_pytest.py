import pytest
from excercises.count_highest import count

def test_count_basic():
    assert count([3,4,5,2,3,8,6,4,5]) == 4

def test_count_empty():
    assert count([]) == 0

def test_count_all_increasing():
    assert count([1,2,3,4,5]) == 5

def test_count_all_same():
    assert count([2,2,2,2]) == 1

def test_count_decreasing():
    assert count([5,4,3,2,1]) == 1
