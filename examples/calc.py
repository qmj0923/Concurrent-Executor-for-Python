import sys
sys.path.append('..')
import os
import random
import time
from util.log import setup_custom_logger
from concurrent_executor import ConcurrentExecutor


def add(a, b):
    time.sleep(0.01)
    return a + b


if __name__ == '__main__':
    root_dir = '../data/calc/'
    kwargs_data = [
        {
            'a': random.randint(1, 100),
            'b': random.randint(1, 100),
        }
        for _ in range(5000)
    ]
    logger = setup_custom_logger(os.path.join(root_dir, 'log.log'))
    executor = ConcurrentExecutor(logger)
    result = executor.run(
        data=kwargs_data,
        func=add,
        output_dir=root_dir,
    )
