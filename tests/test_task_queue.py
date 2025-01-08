from datetime import datetime, timedelta
# supported only from 3.11 onwards:
# from datetime import UTC
# workaround for older versions:
from datetime import timezone
UTC = timezone.utc

import logging
import time
import os
from unittest import mock
from uuid import UUID

import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch

from postgrestq import TaskQueue


POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN", "postgresql://postgres:password@localhost:15432/postgres"
)  # noqa
# We set the lease timeout to 2 seconds, because if the database is slow
# the timeout would be reached while we are still getting the tasks and
# the tests fail.
LEASE_TIMEOUT = 2

logger = logging.getLogger(__name__)


@pytest.fixture
def task_queue():
    queue_name = "test_queue"

    tq = TaskQueue(POSTGRES_DSN, queue_name, reset=True, create_table=True)

    yield tq

    # delete the stuff
    tq._reset()


def test_add(task_queue: TaskQueue):
    # add two tasks and get them back in correct order
    TASKS = [{"foo": 1}, {"bar": 2}, {"d": 56}]
    task_ids = set()
    for idx, task in enumerate(TASKS):
        tid = task_queue.add(
            task,
            LEASE_TIMEOUT,
            can_start_at=datetime.now(UTC) - timedelta(seconds=100) + timedelta(seconds=idx)
        )
        task_ids.add(tid)
    assert len(task_ids) == 3
    task, _, qname = task_queue.get()
    assert task == TASKS[0]
    assert qname == "test_queue"
    task, _, qname = task_queue.get()
    assert task == TASKS[1]
    assert qname == "test_queue"


def test_get(task_queue: TaskQueue):
    TASK = {"foo": 1}
    task_queue.add(TASK, LEASE_TIMEOUT)
    task, _, _ = task_queue.get()
    assert task == TASK
    # calling on empty queue returns None
    assert task_queue.get() == (None, None, None)


def test_is_empty(task_queue: TaskQueue):
    assert task_queue.is_empty()

    task_queue.add({"foo": 1}, LEASE_TIMEOUT)
    assert not task_queue.is_empty()

    task, id_, _qname = task_queue.get()
    assert not task_queue.is_empty()
    assert _qname == "test_queue"
    task_queue.complete(id_)
    assert task_queue.is_empty()


def test_complete(task_queue: TaskQueue):
    # boring case
    task_queue.add({"foo": 1}, LEASE_TIMEOUT, ttl=1)
    _, id_, qname = task_queue.get()
    assert not task_queue.is_empty()
    assert qname == "test_queue"
    task_queue.complete(id_)
    assert task_queue.is_empty()

    # interesting case: we complete the task after it expired already
    task_queue.add({"foo": 1}, LEASE_TIMEOUT, ttl=1)
    _, id_, qname = task_queue.get()
    assert qname == "test_queue"
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert task_queue.is_empty()
    task_queue.complete(id_)
    assert task_queue.is_empty()


def test_expired(task_queue: TaskQueue):
    task_queue.add({"foo": 1}, LEASE_TIMEOUT, ttl=1)
    task_queue.get()
    assert not task_queue.is_empty()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert task_queue.is_empty()

    for i in range(5):
        task_queue.add({str(i): i**2}, LEASE_TIMEOUT)

    tstart = time.time()
    while not task_queue.is_empty():
        task_queue.get()
    tend = time.time()
    assert tend - tstart > LEASE_TIMEOUT


def test_ttl(task_queue: TaskQueue, caplog: LogCaptureFixture):
    task_queue.add({"foo": 1}, LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    # check and put it back into task queue
    assert not task_queue.is_empty()

    # second attempt...
    task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert not task_queue.is_empty()

    # third attempt... *boom*
    task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    caplog.clear()
    assert task_queue.is_empty()
    assert "failed too many times" in caplog.text


def test_callback(task_queue: TaskQueue):
    mock_cb = mock.Mock()
    task_queue.ttl_zero_callback = mock_cb

    task_queue.add({"foo": 1}, LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    # check and put it back into task queue
    assert not task_queue.is_empty()
    assert not mock_cb.called

    # second attempt...
    task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert not task_queue.is_empty()
    assert not mock_cb.called

    # third attempt... *boom*
    task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert task_queue.is_empty()
    assert mock_cb.called


def test_reschedule(task_queue: TaskQueue):
    task_queue.add({"foo": 1}, LEASE_TIMEOUT)
    _, id_, qname = task_queue.get()
    # task queue should be empty as 'foo' is in the processing queue
    assert task_queue.get() == (None, None, None)
    assert qname == "test_queue"
    task_queue.reschedule(id_)
    task, _, qname = task_queue.get()
    assert task == {"foo": 1}
    assert qname == "test_queue"


def test_reschedule_with_ttl(task_queue: TaskQueue):
    task_queue.add({"foo": 1}, LEASE_TIMEOUT, 2)
    _, id_, qname = task_queue.get()
    # task queue should be empty as 'foo' is in the processing queue
    assert task_queue.get() == (None, None, None)
    assert qname == "test_queue"
    task_queue.reschedule(id_, decrease_ttl=True)
    task, _, qname = task_queue.get()
    assert task == {"foo": 1}
    # task queue should be empty because the task is expired(ttl=0)
    assert task_queue.get() == (None, None, None)


def test_reschedule_error(task_queue: TaskQueue):
    with pytest.raises(ValueError):
        task_queue.reschedule("bar")


def test_full(task_queue: TaskQueue):
    TASKS = [
        {"FOO": 1, "f": "something"},
        {"BAR": 2, "b": "something else"},
        {"BAZ": 3, "c": "another thing"},
    ]
    for t in TASKS:
        task_queue.add(t, LEASE_TIMEOUT)

    counter = 0
    while True:
        task, task_id, qname = task_queue.get()
        if task is not None:
            task_queue.complete(task_id)
            counter += 1
        if task_queue.is_empty():
            break

    assert counter == len(TASKS)
    assert qname == "test_queue"


def test_complete_rescheduled_task(task_queue: TaskQueue):
    TASK_CONTENT = {"sloth": 1}
    task_queue.add(TASK_CONTENT, LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    _, task_id, qname = task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)

    # check and put it back into task queue
    assert not task_queue.is_empty()
    assert qname == "test_queue"
    # now the task is completed, although it took a long time...
    task_queue.complete(task_id)

    # it is NOT in the task_queue, because it was finished
    assert task_queue.is_empty()


#
def test_tolerate_double_completion(task_queue: TaskQueue):
    TASK_CONTENT = {"sloth": 1}
    task_queue.add(TASK_CONTENT, LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    task, task_id, qname = task_queue.get()
    assert qname == "test_queue"
    time.sleep(LEASE_TIMEOUT + 0.1)

    # check and put it back into task queue
    assert not task_queue.is_empty()

    # get it again
    _, task_redo_id, qname = task_queue.get()
    assert task_redo_id == task_id
    assert qname == "test_queue"
    # now the task is completed, although it took a long time...
    task_queue.complete(task_id)

    # but the other worker doesn't know and keep processing, until...
    task_queue.complete(task_redo_id)

    # no crashes, the double completion is fine and queues are empty
    assert task_queue.is_empty()


def test_task_queue_len(task_queue: TaskQueue):
    # empty queue
    assert len(task_queue) == 0

    # insert two tasks
    TASKS = [{"foo": 1}, {"bar": 2}]
    for task in TASKS:
        task_queue.add(task, LEASE_TIMEOUT)
    assert len(task_queue) == len(TASKS)

    # removing getting the tasks w/o completing them
    ids = []
    for task in TASKS:
        ids.append(task_queue.get()[1])
    assert len(task_queue) == len(TASKS)

    for id_ in ids:
        task_queue.complete(id_)
    assert len(task_queue) == 0


def test_iterator(task_queue: TaskQueue):
    task_queue.add({"bla": "bla"}, LEASE_TIMEOUT, ttl=3)
    task_queue.add({"blip": "blop"}, LEASE_TIMEOUT, ttl=3)

    found_tasks = []
    for task, id, qname in task_queue:
        found_tasks.append(task)
    assert found_tasks == [{"bla": "bla"}, {"blip": "blop"}]


def test_expired_leases_race(
    task_queue: TaskQueue, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
):
    # save the original function so we can use it inside the mock
    get_orig = task_queue.get_updated_expired_task

    # simulate a race condition in check_expired_leases where we can
    # still see a task in the set of tasks but by the time we try to get
    # it from the queue it has been completed, i.e. is None
    def mock_get(task_id: UUID):
        # marks the task as complete before trying to update the task
        # before trying to update the task
        task_queue.complete(task_id)
        return get_orig(task_id)

    task_queue.add({"foo": 1}, LEASE_TIMEOUT)

    # move task to processing queue
    task_queue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)

    monkeypatch.setattr(task_queue, "get_updated_expired_task", mock_get)
    caplog.set_level(logging.INFO)
    task_queue.check_expired_leases()
    assert "marked completed while we checked for" in caplog.text


def test_lease_timeout_is_none(task_queue: TaskQueue):
    with pytest.raises(TypeError):
        task_queue.add({"bla": "bla", "blip": "blop"}, lease_timeout=None)


def test_lease_timeout_is_not_float_or_int(task_queue: TaskQueue):
    # funny thing, a boolean can be converted to float (i.e. 0.0 and
    # 1.0) without causing an error. so be it
    with pytest.raises(ValueError):
        task_queue.add({"bla": "bla", "blip": "blop"}, lease_timeout="foo")
