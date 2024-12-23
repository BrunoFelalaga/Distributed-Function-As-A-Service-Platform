from dataclasses import dataclass
from typing import Callable, Tuple, List

@dataclass
class TestCase:
    # Represents a single test case with name, function, parameters, and optional description
    name: str
    func: Callable  # Function to be tested
    params: Tuple  # Parameters to pass to the function
    description: str = ""  # Optional description of the test case


def create_cpu_tests() -> List[TestCase]:
    # Create CPU-intensive test cases
    def fibonacci(n: int) -> int:
        # Recursive Fibonacci calculation
        if n <= 1:
            return n
        return fibonacci(n-1) + fibonacci(n-2)
    
    def generate_primes(n: int) -> list:
        # Generate prime numbers up to n
        primes = []
        for num in range(2, n + 1):
            is_prime = True
            for i in range(2, int(num ** 0.5) + 1):
                if num % i == 0:
                    is_prime = False
                    break
            if is_prime:
                primes.append(num)
        return primes

    return [
        TestCase(
            name="fibonacci_30",
            func=fibonacci,
            params=(30,),
            description="Compute 30th Fibonacci number recursively"
        ),
        TestCase(
            name="primes_10000",
            func=generate_primes,
            params=(10000,),
            description="Generate prime numbers up to 10000"
        )
    ]


def create_memory_tests() -> List[TestCase]:
    # Create memory-intensive test cases
    def matrix_operations(size: int) -> list:
        # Perform matrix multiplication
        import numpy as np
        matrix1 = np.random.rand(size, size)
        matrix2 = np.random.rand(size, size)
        result = np.dot(matrix1, matrix2)
        return result.tolist()
    
    def array_manipulations(size: int) -> list:
        # Sort and filter large lists
        import random
        data = [random.random() for _ in range(size)]
        sorted_data = sorted(data)
        filtered = [x for x in sorted_data if x > 0.5]
        return filtered

    return [
        TestCase(
            name="matrix_500x500",
            func=matrix_operations,
            params=(500,),
            description="500x500 matrix multiplication"
        ),
        TestCase(
            name="array_1M",
            func=array_manipulations,
            params=(1_000_000,),
            description="Sort and filter 1M element array"
        )
    ]


def create_noop_tests() -> List[TestCase]:
    # Create a no-operation test case
    def noop():
        # Function that does nothing
        return "noop completed"
    
    return [TestCase(name="noop", func=noop, params=tuple(), description="Immediate return test")]


def create_sleep_tests() -> List[TestCase]:
    def test_sleep(seconds: float) -> str:
        import time
        time.sleep(seconds)
        return f"Slept for {seconds} seconds"
    
    # One simple sleep duration for clearer scaling analysis
    return [TestCase("sleep_test", test_sleep, params=(0.5,))]

def create_multsleep_tests() -> List[TestCase]:
    # Create sleep-based test cases
    def test_sleep(seconds: float) -> str:
        # Function that sleeps for a specified time
        import time
        time.sleep(seconds)
        return f"Slept for {seconds} seconds"
    
    sleep_durations = [0.1, 0.5, 1.0, 2.0]  # Different sleep durations
    return [
        TestCase(
            name=f"sleep_{str(duration).replace('.','pt')}",
            func=test_sleep,
            params=(duration,),
            description=f"Sleep test with {duration}s delay"
        ) for duration in sleep_durations
    ]


def create_failing_tests() -> List[TestCase]:
    # Create a test case that always raises an exception
    def always_fails():
        raise ValueError("This function is designed to fail")

    return [
        TestCase(
            name="always_fail",
            func=always_fails,
            params=tuple(),
            description="Test case that always raises an exception"
        )
    ]


def http_request_test(url="https://httpbin.org/get"):
    import requests
    response = requests.get(url)
    return response.status_code

def api_calls_test(n=10):
    import json
    data = {"test": "data"} * n
    return json.dumps(data)

def db_write_test(n=100):
    import sqlite3
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    c.execute('CREATE TABLE test (id INTEGER, data TEXT)')
    for i in range(n):
        c.execute('INSERT INTO test VALUES (?,?)', (i, f"data_{i}"))
    conn.commit()
    return n

def db_read_test(n=100):
    import sqlite3
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    c.execute('CREATE TABLE test (id INTEGER, data TEXT)')
    c.executemany('INSERT INTO test VALUES (?,?)', 
                 [(i, f"data_{i}") for i in range(n)])
    result = c.execute('SELECT * FROM test').fetchall()
    return len(result)

def db_write_test(n=100):
    import sqlite3
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    c.execute('CREATE TABLE test (id INTEGER, data TEXT)')
    for i in range(n):
        c.execute('INSERT INTO test VALUES (?,?)', (i, f"data_{i}"))
    conn.commit()
    return n

def db_read_test(n=100):
    import sqlite3
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
    c.execute('CREATE TABLE test (id INTEGER, data TEXT)')
    c.executemany('INSERT INTO test VALUES (?,?)', 
                 [(i, f"data_{i}") for i in range(n)])
    result = c.execute('SELECT * FROM test').fetchall()
    return len(result)


def file_write_test(size_mb=1):
    data = b'0' * (size_mb * 1024 * 1024)
    with open('/tmp/test.dat', 'wb') as f:
        f.write(data)
    return size_mb

def file_read_test(size_mb=1):
    with open('/tmp/test.dat', 'rb') as f:
        data = f.read()
    return len(data)



def mixed_load_test():
    # CPU
    result1 = sum(i * i for i in range(10000))
    
    # Memory
    data = [i for i in range(10000)]
    
    # I/O
    with open('/tmp/test.txt', 'w') as f:
        f.write(str(data))
        
    return result1

def bursty_workload_test(burst_size=1000):
    import random
    import time
    
    bursts = []
    for _ in range(3):
        start = time.time()
        result = sum(random.random() for _ in range(burst_size))
        time.sleep(0.1)
        bursts.append(result)
    
    return bursts


# def create_io_tests():
#     return [
#         TestCase("network_io", http_request_test),
#         TestCase("db_ops", db_write_test, (1000,)),
#         TestCase("file_io", file_write_test, (5,))
#     ]

def create_io_tests():
    return [
        TestCase("network_io", http_request_test, params=None),
        TestCase("db_ops", db_write_test, params=(1000,)),
        TestCase("file_io", file_write_test, params=(5,))
    ]

def create_mixed_tests():
    return [
        TestCase("mixed_load", mixed_load_test, params=None),
        TestCase("bursty_load", bursty_workload_test, (5000,))
    ]


def create_weak_scaling_tests():
    base_tasks = 5  # Tasks per worker process
    configs = []
    for workers in [1, 2, 4, 8]:
        total_tasks = workers * base_tasks
        configs.append((workers, total_tasks))
    return configs