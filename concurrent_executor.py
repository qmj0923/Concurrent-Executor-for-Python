'''
Copyright (c) 2023 qmj0923

https://github.com/qmj0923/Concurrent-Executor-for-Python
'''

from __future__ import annotations
import collections
import inspect
import io
import json
import logging
import os
import shutil

from tqdm import tqdm
from tqdm.contrib.concurrent import process_map as tqdm_map


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
        self, logger: logging.Logger = None,
        response_key='response', separator=' | '
    ):
        '''
        Parameters
        ----------
        response_key: key of a dictionary that records the result of execution
            Change if it confilcts with any parameter of the function that to
            be executed. Otherwise the default value is recommended.

        separator: string to join the input arguments of the function
            Change if it confilcts with any possible argument.
        '''
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
        self.response_key = response_key
        self.separator = separator
        
    def load(self, fname):
        with open(fname, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def dump(self, obj, fname):
        with open(fname, 'w', encoding='utf-8') as f:
            return json.dump(obj, f, ensure_ascii=False, indent=2)

    def encode_arguments(self, params: list) -> str:
        return self.separator.join([str(param) for param in params])

    def decode_arguments(self, key: str) -> list[str]:
        return key.split(self.separator)

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
        part_fname = os.path.join(output_dir, f'part_{seq}.json')
        error_fname = os.path.join(output_dir, f'error_{seq}.json')
        log_fname = os.path.join(output_dir, f'log_{seq}.log')
        if os.path.exists(part_fname):
            return
        result, errors = list(), list()
        with open(log_fname, 'w', encoding='utf-8') as f:
            for kwargs in tqdm(kwargs_data, file=f):
                try:
                    response = func(**kwargs)
                except Exception as e:
                    response = None
                    errors.append({'kwargs': kwargs, 'msg': str(e)})
                result.append({**kwargs, **{self.response_key: response}})
        self.dump(result, part_fname)
        if errors:
            self.dump(errors, error_fname)

    def _work_wrapper(self, kwargs: dict):
        return self._worker(**kwargs)

    def _collate_result(
        self, func: function, tmp_dir: str,
        return_format: str, default_response
    ) -> list[dict] | dict | None:
        if return_format not in ['list', 'dict', 'none']:
            raise ValueError(
                '[Concurrent Executor] '
                'Invalid return_format.'
            )
        if return_format == 'none':
            return None
        segment_list = [
            os.path.join(tmp_dir, path)
            for path in os.listdir(tmp_dir)
            if path.startswith('part_') and path.endswith('.json')
        ]
        result = list()
        for fname in segment_list:
            result += self.load(fname)
        for item in result:
            if item[self.response_key] is None:
                item[self.response_key] = default_response

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
                ]): info_dict[self.response_key]
                for info_dict in result
            }
        return result
    
    def _collate_error(self, tmp_dir: str) -> list[str]:
        segment_list = [
            os.path.join(tmp_dir, path)
            for path in os.listdir(tmp_dir)
            if path.startswith('error_') and path.endswith('.json')
        ]
        errors = list()
        for fname in segment_list:
            errors += self.load(fname)
        if errors:
            for e in errors:
                self.logger.error(f'[{str(e["kwargs"])}] {str(e["msg"])}')
            self.dump(errors, os.path.join(tmp_dir, 'error.json'))
        return errors

    def run(
        self, data: list, func: function, output_dir: str,
        return_format='list', default_response=None,
        batch_size=1000, max_workers=8
    ) -> list[dict] | dict | None:
        '''
        Parameters
        ----------
        func: function to be executed

        data: list of `func`'s inputs that are already arranged into either
        argument lists or keyword argument lists

        output_dir: the directory to save files

        return_format: the format of the return value
        
        default_response: the default return value of `func`
            If the return value of `func` is None, it will be set to
            `default_response`.

        batch_size: the number of `func`'s inputs to be processed by each
        worker at a time

        max_workers: the maximum number of workers that can be used

        Returns
        -------
        Set `return_format` to 'none' if you don't want to return anything.
        
        If `return_format` == 'list', the return value will be a list of
        dictionaries as follows.
        ```
        [
            {
                param1: arg1,
                param2: arg2,
                ...
                self.response_key: func(arg1, arg2, ...),
            },
            ...
        ]
        ```
        If `return_format` == 'dict', the return value will be a dictionary
        whose keys are the arguments of `func` and whose values are the
        corresponding return values of `func`, as shown below.
        ```
        {
            arg1 + self.separator + arg2 + ...: func(arg1, arg2, ...),
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
        self.logger.info(
            f'Data will be divided into {iteration} parts, with a maximum '
            f'of {batch_size} item(s) in each part.'
        )
        self.logger.info(f'Temporary files will be saved in {tmp_dir}.')
        self.logger.info('Start executing...')

        tqdm_out = TqdmToLogger(self.logger)
        tqdm_map(
            self._work_wrapper,
            [{
                'kwargs_data': kwargs_data[
                    i * batch_size:(i + 1) * batch_size
                ],
                'func': func, 'seq': i, 'output_dir': tmp_dir,
            } for i in range(iteration)],
            max_workers=max_workers,
            file=tqdm_out,
        )
        self._collate_error(tmp_dir)
        result = self._collate_result(
            func, tmp_dir, return_format, default_response
        )
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
