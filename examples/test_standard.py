import sys
sys.path.append('..')
import os
import time
from util.log import setup_custom_logger
from concurrent_executor import ConcurrentExecutor


def foo(x):
    if x % 300 == 7:
        raise ValueError('foo')
    time.sleep(0.01)
    return x


if __name__ == '__main__':
    root_dir = '../data/standard/'
    logger = setup_custom_logger(os.path.join(root_dir, 'log.log'))
    executor = ConcurrentExecutor(logger)
    result = executor.run(
        data=[[i] for i in range(5000)],
        func=foo,
        output_dir=root_dir,
    )
    executor.dump(result, os.path.join(root_dir, 'result.json'))
