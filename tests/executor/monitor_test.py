from __future__ import annotations

from magnify.config import MonitorConfig

from taps.executor.monitoring import MonitoringExecutorConfig
from taps.executor.python import ThreadPoolConfig


def test_monitor_executor(tmpdir) -> None:
    base_executor = ThreadPoolConfig(max_threads=1)
    print(tmpdir.relto(''))
    monitor_config = MonitorConfig(
        sensors=[{'kind': 'psutil'}],
        stores=[{'kind': 'file', 'options': {'dir_name': tmpdir.relto('')}}],
    )
    config = MonitoringExecutorConfig(
        executor=base_executor,
        monitor=monitor_config,
    )

    executor = config.get_executor()
    assert isinstance(repr(executor), str)

    with executor:
        future = executor.submit(sum, [1, 2], start=-3)
        assert future.result() == 0

        future = executor.submit(sum, [1, 2, 3], start=-6)
        assert future.result() == 0

        output = executor.map(abs, [1, -1])
        assert list(output) == [1, 1]
