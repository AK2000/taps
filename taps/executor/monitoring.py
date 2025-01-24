from __future__ import annotations

import sys
from concurrent.futures import Executor
from concurrent.futures import Future
from types import TracebackType
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import Literal
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from magnify.client import magnify_decorator
from magnify.config import MonitorConfig
from magnify.monitor import MagnifyMonitor
from pydantic import Field

from taps.executor import ExecutorConfig
from taps.logging import get_repr
from taps.plugins import register

P = ParamSpec('P')
T = TypeVar('T')


class MonitoringExecutor(Executor):
    """Executor wrapper that add resource monitoring to any executor.

    Args:
        executor: Executor to wrap.
        monitor: instance of magnify monitor to use with test
    """

    def __init__(self, executor: Executor, monitor: MagnifyMonitor) -> None:
        self.executor = executor
        self.monitor = monitor
        self.monitor.start()

    def __enter__(self) -> Self:
        self.executor.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> bool | None:
        return self.executor.__exit__(exc_type, exc_value, exc_traceback)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(executor={get_repr(self.executor)})'

    def _get_wrapped(self, fn: Callable[P, T]) -> Callable[P, T]:
        return magnify_decorator(fn)

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        """Schedule the callable to be executed.

        Args:
            function: Callable to execute.
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            [`Future`][concurrent.futures.Future] object representing the \
            result of the execution of the callable.
        """
        wrapped_function = self._get_wrapped(function)
        return self.executor.submit(wrapped_function, *args, **kwargs)

    def map(
        self,
        function: Callable[P, T],
        *iterables: Iterable[P.args],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Map a function onto iterables of arguments.

        Args:
            function: A callable that will take as many arguments as there are
                passed iterables.
            iterables: Variable number of iterables.
            timeout: The maximum number of seconds to wait. If None, then there
                is no limit on the wait time.
            chunksize: If greater than one, the iterables will be chopped into
                chunks of size chunksize and submitted to the executor. If set
                to one, the items in the list will be sent one at a time.

        Returns:
            An iterator equivalent to: `map(func, *iterables)` but the calls \
            may be evaluated out-of-order.

        Raises:
            ValueError: if chunksize is less than one.
        """
        wrapped_function = self._get_wrapped(function)
        return self.executor.map(
            wrapped_function,
            *iterables,
            timeout=timeout,
            chunksize=chunksize,
        )

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        """Shutdown the executor.

        Args:
            wait: Wait on all pending futures to complete.
            cancel_futures: Cancel all pending futures that the executor
                has not started running. Only used in Python 3.9 and later.
        """
        if sys.version_info >= (3, 9):  # pragma: >=3.9 cover
            self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        else:  # pragma: <3.9 cover
            self.executor.shutdown(wait=wait)


@register('executor')
class MonitoringExecutorConfig(ExecutorConfig):
    """[`MonitoringExecutor`][taps.executor.dask.MonitoringExecutor] plugin configuration."""  # noqa: E501

    executor: ExecutorConfig
    monitor: MonitorConfig
    name: Literal['monitoring_wrapper'] = Field(
        'monitoring_wrapper',
        description='Executor name.',
    )

    def get_executor(self) -> MonitoringExecutor:
        """Create an executor instance from the config."""
        wrapped_executor = self.executor.get_executor()
        monitor = self.monitor.get_monitor()

        return MonitoringExecutor(wrapped_executor, monitor)
