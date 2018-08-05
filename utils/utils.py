import itertools
import time


def flatten_list(lst):
    return list(itertools.chain.from_iterable(lst))


def split_to_chunks(item_list, chunk_size, transform_fn=lambda x: x):
    total_size = len(item_list)
    if total_size <= chunk_size:
        chunks = [transform_fn(item_list)]
    else:
        borders = [(i, i + chunk_size) for i in range(0, total_size, chunk_size)]
        chunks = [transform_fn(item_list[left:right]) for left, right in borders]
    return chunks


def timestamp():
    now = time.time()
    return time.strftime('%Y-%m-%d %H:%M:%S') + '{:03d}'.format(int((now - int(now)) * 1000))
