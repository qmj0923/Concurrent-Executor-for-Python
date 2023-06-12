# Concurrent Executor  

Concurrently execute a function with a list of function inputs.

## Features

1. This framework can apply to any callable object.
2. Checkpoints will be saved and loaded automatically.
3. The real-time progress bar will be logged to `<output_dir>/_tmp/log_<i>.log`.

## Example

Given a function foo(), our goal is to execute foo(0), foo(1), ..., foo(4999) concurrently.

```python
import time
from concurrent_executor import ConcurrentExecutor

def foo(x):
    if x % 300 == 7:
        raise ValueError('foo')
    time.sleep(0.01)
    return x

if __name__ == '__main__':
    executor = ConcurrentExecutor()
    result = executor.run(
        data=[[i] for i in range(5000)],
        func=foo,
        output_dir='data/',
    )
```

## Run the given examples

Suppose that we will run `examples/test_standard.py`.

```bash
$ cd examples
$ python test_standard.py
```

## Tips

If you're concerned about the size of `<output_dir>/_tmp/`, consider the following suggestions:

Check if the return value of your function is too large. If so, consider storing it to a file instead, as demonstrated in `examples/crawl.py`.
Check if the input to your function is too large. If so, consider storing the input to a file and passing the file path to your function instead.
