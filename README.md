# Concurrent Executor  

Concurrently execute a function with a list of function inputs.



## Example

Given a function foo(), our goal is to execute foo(0), foo(1), ..., foo(4999) concurrently.

```python
import time, logging
from concurrent_executor import ConcurrentExecutor

def foo(x):
    if x % 300 == 7:
        raise ValueError('foo')
    time.sleep(0.01)
    return x

if __name__ == '__main__':
    executor = ConcurrentExecutor(logging.getLogger())
    result = executor.run(
        data=[[i] for i in range(5000)],
        func=foo,
        output_dir='data/',
    )
```

