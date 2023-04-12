from pathlib import Path
import collections
import contextlib
import cProfile
import datetime
import time

import pytest

from hat import aio


durations = collections.deque()


def pytest_addoption(parser):
    parser.addoption("--perf",
                     action="store_true",
                     help="run performance tests")


def pytest_configure(config):
    aio.init_asyncio()
    config.addinivalue_line("markers", "perf: mark performance test")


def pytest_runtest_setup(item):
    is_perf_run = item.config.getoption("--perf")
    is_perf_test = any(item.iter_markers(name="perf"))
    if is_perf_run and not is_perf_test:
        pytest.skip("running only performance tests")
    elif not is_perf_run and is_perf_test:
        pytest.skip("not running performance tests")


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if not durations:
        return
    terminalreporter.write('\nDuration report:\n')
    for i in durations:
        identifier = i['identifier']
        description = i['description']
        dt = datetime.timedelta(seconds=i['dt'])
        terminalreporter.write(f"> {dt} [{identifier}] {description}\n")


@pytest.fixture
def duration(request):
    identifier = request.module.__name__
    if request.cls:
        identifier += f"::{request.cls.__name__}"
    if request.function:
        identifier += f"::{request.function.__name__}"

    @contextlib.contextmanager
    def duration(description):
        start = time.monotonic()
        yield
        dt = time.monotonic() - start
        durations.append({'identifier': identifier,
                          'description': description,
                          'dt': dt})

    return duration


@pytest.fixture
def profile(request):

    @contextlib.contextmanager
    def profile(name=None):
        with cProfile.Profile() as pr:
            yield

        suffix = f'.{name}.prof' if name else '.prof'
        path = (Path(__file__).parent / 'profile' /
                Path(*request.module.__name__.split('.')) /
                request.function.__name__).with_suffix(suffix)

        path.parent.mkdir(parents=True, exist_ok=True)
        pr.dump_stats(str(path))

    return profile
