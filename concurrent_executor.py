from __future__ import annotations
import inspect
import collections
import os
import shutil
import logging
from tqdm import tqdm
# from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from concurrent.futures import ProcessPoolExecutor as PoolExecutor
from util.file_io import load_json, dump_json


class ConcurrentExecutor:
    '''
    Concurrently execute a function with a list of function inputs.

    Example
    ----------
    Given a function foo(), our goal is to execute foo(0), foo(1), ...,
    foo(4999) concurrently.

    ```
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

    See ConcurrentExecutor.run() for more details.
    '''

    def __init__(self, logger: logging.Logger):
        # Since logging in a multiprocessing setup is not safe, using it in
        # self._worker() is not recommended. See the following link for more
        # details.
        # https://stackoverflow.com/questions/1154446/is-file-append-atomic-in-unix
        self.logger = logger
        
    def load(self, fname):
        return load_json(fname)
    
    def dump(self, obj, fname):
        dump_json(obj, fname)

    def encode_arguments(self, params: list) -> str:
        return ' | '.join([str(param) for param in params])

    def decode_arguments(self, key: str) -> list[str]:
        return key.split(' | ')

    def _convert_to_kwargs_data(
        self, data: list, func: function
    ) -> list[dict]:
        if not data:
            return list()
        if isinstance(data[0], dict):
            # The elements in data are already in the form of kwargs.
            return data
        if isinstance(data[0], collections.abc.Sequence):
            # The elements in data are in the form of args.
            sig = inspect.signature(func)
            # https://docs.python.org/3/library/inspect.html#inspect.BoundArguments
            return [dict(sig.bind(*args).arguments) for args in data]
        raise ValueError(
            '[Concurrent Executor] '
            'The elements in data must be in the form of args or kwargs.'
        )

    def _worker(
        self, kwargs_data: list[dict], func: function,
        seq: int, output_dir: str
    ):
        tmp_file = os.path.join(output_dir, f'part_{seq}.json')
        error_file = os.path.join(output_dir, f'error_{seq}.json')
        log_file = os.path.join(output_dir, f'log_{seq}.log')
        if os.path.exists(tmp_file):
            return
        result, errors = list(), list()
        with open(log_file, 'w', encoding='utf-8') as f:
            for kwargs in tqdm(kwargs_data, file=f):
                try:
                    response = func(**kwargs)
                except Exception as e:
                    errors.append(f'[{str(kwargs)}] {str(e)}')
                    continue
                result.append({**kwargs, **{'response': response}})
        self.dump(result, tmp_file)
        if errors:
            self.dump(errors, error_file)

    def _work_wrapper(self, kwargs: dict):
        return self._worker(**kwargs)

    def _collate_result(
        self, func: function, tmp_dir: str, return_format: str
    ) -> list[dict] | dict:
        if return_format not in ['list', 'dict']:
            raise ValueError(
                '[Concurrent Executor] '
                'return_format must be "list" or "dict".'
            )
        segment_files = [
            os.path.join(tmp_dir, file)
            for file in os.listdir(tmp_dir)
            if file.startswith('part_') and file.endswith('.json')
        ]
        result = list()
        for fname in segment_files:
            result += self.load(fname)

        if return_format == 'list':
            pass
        if return_format == 'dict':
            sig = inspect.signature(func)
            # https://docs.python.org/3/library/inspect.html#inspect.Parameter
            result = {
                self.encode_arguments([
                    str(info_dict[param])
                    for param in sig.parameters.keys()
                    if param in info_dict
                ]): info_dict['response']
                for info_dict in result
            }
        return result
    
    def _collate_error(self, tmp_dir: str) -> list[str]:
        segment_files = [
            os.path.join(tmp_dir, file)
            for file in os.listdir(tmp_dir)
            if file.startswith('error_') and file.endswith('.json')
        ]
        errors = list()
        for fname in segment_files:
            errors += self.load(fname)
        for error in errors:
            self.logger.error(error)
        return errors

    def run(
        self, data: list, func: function, output_dir: str,
        return_format='list', batch_size=1000, max_workers=8
    ) -> list[dict] | dict:
        '''
        Parameters
        ----------
        func: function to be executed

        data: list of the function inputs are already arranged into either
        argument lists or keyword argument lists

        output_dir: the directory to save files

        return_format: the format of the return value

        batch_size: the number of function inputs to be processed by each
        worker at a time

        max_workers: the maximum number of workers that can be used

        Returns
        -------
        If `return_format` == 'list', the return value will be a list of
        dictionaries as follows.
        ```
        [
            {
                param1: arg1,
                param2: arg2,
                ...
                'response': func(arg1, arg2, ...),
            },
            ...
        ]
        ```
        If `return_format` == 'dict', the return value will be a dictionary
        whose keys are the arguments of `func` and whose values are the
        corresponding return values of `func`, as shown below.
        ```
        {
            arg1 + ' | ' + arg2 + ' | ' + ...: func(arg1, arg2, ...),
            ...
        }
        ```
        '''
        kwargs_data = self._convert_to_kwargs_data(data, func)
        total_len = len(kwargs_data)
        iteration = (total_len + batch_size - 1) // batch_size
        tmp_dir = os.path.join(output_dir, '_tmp/')
        os.makedirs(tmp_dir, exist_ok=True)

        self.logger.info(f'Executing "{func.__name__}" for {total_len} data.')
        self.logger.info(f'Temporary files will be saved in {tmp_dir}.')
        self.logger.info('Start executing...')
        with PoolExecutor(max_workers=max_workers) as executor:
            # Don't take lambda as the first argument of ProcessPoolExecutor.map().
            executor.map(
                self._work_wrapper,
                [{
                    'kwargs_data': kwargs_data[
                        i * batch_size:(i + 1) * batch_size
                    ],
                    'func': func, 'seq': i, 'output_dir': tmp_dir,
                } for i in range(iteration)]
            )
        self._collate_error(tmp_dir)
        result = self._collate_result(func, tmp_dir, return_format)
        # self.dump(result, os.path.join(output_dir, 'result.json'))
        # shutil.rmtree(tmp_dir)
        return result
