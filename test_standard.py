import time
from log import setup_custom_logger
from concurrent_executor import ConcurrentExecutor


def foo(x):
    if x % 300 == 7:
        raise ValueError('foo')
    time.sleep(0.01)
    return x


if __name__ == '__main__':
    logger = setup_custom_logger('data/standard/test.log')
    executor = ConcurrentExecutor(logger)
    result = executor.run(
        data=[[i] for i in range(5000)],
        func=foo,
        output_dir='data/standard/',
    )
    executor.dump(result, 'data/standard/result.json')
