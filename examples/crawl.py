import sys
sys.path.append('..')
import os
import requests
from util.log import setup_custom_logger
from concurrent_executor import ConcurrentExecutor

MONTH = [
    'January', 'February', 'March', 'April',
    'May', 'June', 'July', 'August',
    'September', 'October', 'November', 'December'
]


def crawl_page(url, output_path):
    # https://github.com/MatsuriDayo/nekoray/issues/104
    response = requests.get(url, proxies={
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    })
    if response.status_code != requests.codes.ok:
        raise Exception(f"Network error. Status code: {response.status_code}")
    html = response.text
    # return html
    if dst_dir := os.path.dirname(output_path):
        os.makedirs(dst_dir, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)


if __name__ == '__main__':
    root_dir = '../data/crawl/'
    args_data = [
        [
            (f'https://en.wikipedia.org/wiki/'
            'Portal:Current_events/{month}_{year}'),
            os.path.join(root_dir, f'{year}', f'{month}.html'),
        ]
        for year in range(2011, 2021)
        for month in MONTH
    ]
    logger = setup_custom_logger(os.path.join(root_dir, 'log.log'))
    executor = ConcurrentExecutor(logger)
    executor.run(
        data=args_data,
        func=crawl_page,
        output_dir=root_dir,
        return_format='none',
        batch_size=20,
    )


'''
# The code below is an alternative implementation of the above code.

import os
import requests
from tqdm.contrib.concurrent import process_map as tqdm_map

MONTH = [
    'January', 'February', 'March', 'April',
    'May', 'June', 'July', 'August',
    'September', 'October', 'November', 'December'
]


def crawl_page(args):
    url, output_path = args
    if os.path.exists(output_path):
        return
    # https://github.com/MatsuriDayo/nekoray/issues/104
    response = requests.get(url, proxies={
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    })
    if response.status_code != requests.codes.ok:
        raise Exception(f"Network error. Status code: {response.status_code}")
    html = response.text
    if dst_dir := os.path.dirname(output_path):
        os.makedirs(dst_dir, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)


if __name__ == '__main__':
    root_dir = '../data/crawl/'
    args_data = [
        [
            (f'https://en.wikipedia.org/wiki/'
            'Portal:Current_events/{month}_{year}'),
            os.path.join(root_dir, f'{year}', f'{month}.html'),
        ]
        for year in range(2011, 2021)
        for month in MONTH
    ]
    tqdm_map(crawl_page, args_data, max_workers=8, chunksize=len(args_data) // 20 + 1)
'''
