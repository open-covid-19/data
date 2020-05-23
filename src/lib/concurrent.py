from typing import Any, Callable, Iterable
from tqdm.contrib import concurrent
from multiprocess.pool import Pool, ThreadPool


class _ProcessExecutor(Pool):
    def __init__(self, max_workers: int = None, **kwargs):
        super().__init__(processes=max_workers, **kwargs)

    def map(self, map_func: Callable, map_iter: Iterable[Any]):
        return self.imap(map_func, map_iter)


class _ThreadExecutor(ThreadPool):
    def __init__(self, max_workers: int = None, **kwargs):
        super().__init__(processes=max_workers, **kwargs)

    def map(self, map_func: Callable, map_iter: Iterable[Any]):
        return self.imap(map_func, map_iter)


def process_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    return concurrent._executor_map(_ProcessExecutor, map_func, map_iter, **tqdm_kwargs)


def thread_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    return concurrent._executor_map(_ThreadExecutor, map_func, map_iter, **tqdm_kwargs)
