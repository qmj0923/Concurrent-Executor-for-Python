'''
Copyright (c) 2023 qmj0923

https://github.com/qmj0923/Concurrent-Executor-for-Python
'''

from __future__ import annotations
import inspect
import io
import json
import logging
import os
import shutil

from collections.abc import Sequence
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map as tqdm_map
from typing import Callable, Optional


class ConcurrentExecutor:
    '''
    Concurrently execute a function with a list of function inputs.

    Example
    ----------
    Given a function foo(), our goal is to execute foo(0), foo(1), ...,
    foo(4999) concurrently.

    ```
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

    See ConcurrentExecutor.run() for more details.
    '''

    def __init__(
        self, logger: Optional[logging.Logger] = None
    ):
        # Since logging in a multiprocessing setup is not safe, using it in
        # self._worker() is not recommended. See the following link for more
        # details.
        # https://stackoverflow.com/questions/47968861/does-python-logging-support-multiprocessing
        if logger is None:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
            self.logger.addHandler(logging.StreamHandler())  # Write to stdout.
        else:
            self.logger = logger
        self.response_key = '#response'
        
    def load(self, fname):
        with open(fname, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def dump(self, obj, fname):
        with open(fname, 'w', encoding='utf-8') as f:
            return json.dump(obj, f, ensure_ascii=False, indent=2)

    def _convert_to_kwargs_data(
        self, data: list, func: Callable
    ) -> list[dict]:
        if not data:
            return list()
        if isinstance(data[0], dict):
            # The elements in data are already in the form of kwargs.
            return data
        if isinstance(data[0], Sequence):
            # The elements in data are in the form of args.
            sig = inspect.signature(func)
            # https://docs.python.org/3/library/inspect.html#inspect.BoundArguments
            return [dict(sig.bind(*args).arguments) for args in data]
        raise ValueError(
            '[Concurrent Executor] '
            'The elements in data must be in the form of args or kwargs.'
        )

    def _worker(
        self, kwargs_data: list[dict], func: Callable,
        seq: int, output_dir: str
    ):
        part_fname = os.path.join(output_dir, f'part_{seq}.json')
        error_fname = os.path.join(output_dir, f'error_{seq}.json')
        log_fname = os.path.join(output_dir, f'log_{seq}.log')
        if os.path.exists(part_fname):
            return
        result, errors = list(), list()
        with open(log_fname, 'w', encoding='utf-8') as f:
            for i, kwargs in enumerate(tqdm(kwargs_data, file=f)):
                try:
                    response = func(**kwargs)
                except Exception as e:
                    response = self.default_response
                    errors.append({'id': i, 'kwargs': kwargs, 'msg': str(e)})
                result.append({**kwargs, **{self.response_key: response}})
        self.dump(result, part_fname)
        if errors:
            self.dump(errors, error_fname)

    def _work_wrapper(self, kwargs: dict):
        return self._worker(**kwargs)

    def _collate_result(self, tmp_dir: str) -> list:
        segment_list = [
            os.path.join(tmp_dir, path) for path in os.listdir(tmp_dir)
            if path.startswith('part_') and path.endswith('.json')
        ]
        result = list()
        for fname in segment_list:
            result += [item[self.response_key] for item in self.load(fname)]
        return result
    
    def _collate_error(self, tmp_dir: str, batch_size: int) -> list:
        segment_list = [
            os.path.join(tmp_dir, path) for path in os.listdir(tmp_dir)
            if path.startswith('error_') and path.endswith('.json')
        ]
        errors = list()
        for fname in segment_list:
            seq = int(fname.split('_')[-1].split('.')[0])
            for e in self.load(fname):
                e['id'] += seq * batch_size
                errors.append(e)
        if errors:
            for e in errors:
                self.logger.error(
                    f'[Index {e["id"]}, kwargs: {e["kwargs"]}] {e["msg"]}'
                )
            self.dump(errors, os.path.join(tmp_dir, 'error.json'))
        return errors

    def run(
        self, data: list, func: Callable, output_dir: str, max_workers=8,
        batch_size=1000, default_response=None, do_return=True
    ) -> list | None:
        '''
        Parameters
        ----------
        func: function to be executed

        data: list of `func`'s inputs that are already arranged into either
        argument lists or keyword argument lists

        output_dir: the directory to save files
        
        max_workers: the maximum number of workers that can be used

        batch_size: the number of `func`'s inputs to be processed by each
        worker at a time

        default_response: the default return value of `func` when an error
        occurs during its execution.

        do_return: whether to return the result of `func`'s execution

        Returns
        -------
        A list containing the result of applying `func` to each element of
        `data`, similar to the built-in `map` function.
        '''
        kwargs_data = self._convert_to_kwargs_data(data, func)
        total_len = len(kwargs_data)
        iteration = (total_len + batch_size - 1) // batch_size

        self.default_response = default_response
        tmp_dir = os.path.join(output_dir, '_tmp/')
        os.makedirs(tmp_dir, exist_ok=True)

        self.logger.info(f'Executing "{func.__name__}" for {total_len} data.')
        self.logger.info(
            f'Data will be divided into {iteration} parts, with a maximum '
            f'of {batch_size} item(s) in each part.'
        )
        self.logger.info(f'Temporary files will be saved in {tmp_dir}.')
        self.logger.info('Start executing...')

        # Don't take lambda as the first argument.
        tqdm_map(
            self._work_wrapper,
            [{
                'kwargs_data': kwargs_data[
                    i * batch_size:(i + 1) * batch_size
                ],
                'func': func, 'seq': i, 'output_dir': tmp_dir,
            } for i in range(iteration)],
            max_workers=max_workers,
            file=TqdmToLogger(self.logger),
        )
        self._collate_error(tmp_dir, batch_size)

        if not do_return:
            return
        result = self._collate_result(tmp_dir)
        # self.dump(result, os.path.join(output_dir, 'result.json'))
        # shutil.rmtree(tmp_dir)
        return result


class TqdmToLogger(io.StringIO):
    """
    Output stream for TQDM which will output to logger module instead of
    the StdOut.
    
    https://github.com/tqdm/tqdm/issues/313#issuecomment-267959111
    """
    def __init__(self, logger: logging.Logger):
        super().__init__()
        self.logger = logger

    def write(self, buf):
        self.buf = buf.strip('\r\n\t ')

    def flush(self):
        self.logger.log(self.logger.level, self.buf)
