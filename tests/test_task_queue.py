import logging
import time
import os
from unittest import mock

import pytest

from postgrestq import TaskQueue


POSTGRES_DSN = os.environ.get(
    'POSTGRES_DSN',
    'postgresql://postgres:password@localhost:5432/postgres'
)  # noqa
LEASE_TIMEOUT = 0.1

logger = logging.getLogger(__name__)


@pytest.fixture
def taskqueue():

    # queue_name = str(uuid.uuid4())
    queue_name = 'test_queue'

    tq = TaskQueue(POSTGRES_DSN, queue_name)

    yield tq

    # delete the stuff
    tq._reset()


def test_add(taskqueue):
    # add two tasks and get them back in correct order
    TASKS = ['foo', 'bar']
    for task in TASKS:
        taskqueue.add(task, LEASE_TIMEOUT)

    task, _ = taskqueue.get()
    assert task == TASKS[0]
    task, _ = taskqueue.get()
    assert task == TASKS[1]


def test_get(taskqueue):
    TASK = 'foo'
    taskqueue.add(TASK, LEASE_TIMEOUT)
    task, _ = taskqueue.get()
    assert task == TASK
    # calling on empty queue returns None
    assert taskqueue.get() == (None, None)


def test_is_empty(taskqueue):
    assert taskqueue.is_empty()

    taskqueue.add('foo', LEASE_TIMEOUT)
    assert not taskqueue.is_empty()

    task, id_ = taskqueue.get()
    assert not taskqueue.is_empty()

    taskqueue.complete(id_)
    assert taskqueue.is_empty()


def test_complete(taskqueue):
    # boring case
    taskqueue.add('foo', LEASE_TIMEOUT, ttl=1)
    _, id_ = taskqueue.get()
    assert not taskqueue.is_empty()
    taskqueue.complete(id_)
    assert taskqueue.is_empty()

    # interesting case: we complete the task after it expired already
    taskqueue.add('foo', LEASE_TIMEOUT, ttl=1)
    _, id_ = taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert taskqueue.is_empty()
    taskqueue.complete(id_)
    assert taskqueue.is_empty()


def test_expired(taskqueue):
    taskqueue.add('foo', LEASE_TIMEOUT, ttl=1)
    taskqueue.get()
    assert not taskqueue.is_empty()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert taskqueue.is_empty()

    for i in range(5):
        taskqueue.add(i, LEASE_TIMEOUT)

    tstart = time.time()
    while not taskqueue.is_empty():
        taskqueue.get()
    tend = time.time()
    assert tend - tstart > LEASE_TIMEOUT


def test_ttl(taskqueue, caplog):
    taskqueue.add('foo', LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    # check and put it back into task queue
    assert not taskqueue.is_empty()

    # second attempt...
    taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert not taskqueue.is_empty()

    # third attempt... *boom*
    taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    caplog.clear()
    assert taskqueue.is_empty()
    assert "failed too many times" in caplog.text


def test_callback(taskqueue):

    mock_cb = mock.Mock()
    taskqueue.ttl_zero_callback = mock_cb

    taskqueue.add('foo', LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    # check and put it back into task queue
    assert not taskqueue.is_empty()
    assert not mock_cb.called

    # second attempt...
    taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert not taskqueue.is_empty()
    assert not mock_cb.called

    # third attempt... *boom*
    taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)
    assert taskqueue.is_empty()
    assert mock_cb.called


def test_reschedule(taskqueue):
    taskqueue.add('foo', LEASE_TIMEOUT)
    _, id_ = taskqueue.get()
    # task queue should be empty as 'foo' is in the processing queue
    assert taskqueue.get() == (None, None)

    taskqueue.reschedule(id_)
    task, _ = taskqueue.get()
    assert task == 'foo'


def test_reschedule_error(taskqueue):
    with pytest.raises(ValueError):
        taskqueue.reschedule('bar')


def test_full(taskqueue):
    TASKS = ['FOO', 'BAR', 'BAZ']
    for t in TASKS:
        taskqueue.add(t, LEASE_TIMEOUT)

    counter = 0
    while True:
        task, task_id = taskqueue.get()
        if task is not None:
            taskqueue.complete(task_id)
            counter += 1
        if taskqueue.is_empty():
            break

    assert counter == len(TASKS)


def test_complete_rescheduled_task(taskqueue):
    TASK_CONTENT = 'sloth'
    taskqueue.add(TASK_CONTENT, LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    _, task_id = taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)

    # check and put it back into task queue
    assert not taskqueue.is_empty()

    # now the task is completed, although it took a long time...
    taskqueue.complete(task_id)

    # it is NOT in the taskqueue, because it was finished
    assert taskqueue.is_empty()


#
def test_tolerate_double_completion(taskqueue):
    TASK_CONTENT = 'sloth'
    taskqueue.add(TASK_CONTENT, LEASE_TIMEOUT, ttl=3)

    # start a task and let it expire...
    task, task_id = taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)

    # check and put it back into task queue
    assert not taskqueue.is_empty()

    # get it again
    _, task_redo_id = taskqueue.get()
    assert task_redo_id == task_id

    # now the task is completed, although it took a long time...
    taskqueue.complete(task_id)

    # but the other worker doesn't know and keep processing, until...
    taskqueue.complete(task_redo_id)

    # no crashes, the double completion is fine and queues are empty
    assert taskqueue.is_empty()


def test_task_queue_len(taskqueue):

    # empty queue
    assert len(taskqueue) == 0

    # insert two tasks
    TASKS = ['foo', 'bar']
    for task in TASKS:
        taskqueue.add(task, LEASE_TIMEOUT)
    assert len(taskqueue) == len(TASKS)

    # removing getting the tasks w/o completing them
    ids = []
    for task in TASKS:
        ids.append(taskqueue.get()[1])
    assert len(taskqueue) == len(TASKS)

    for id_ in ids:
        taskqueue.complete(id_)
    assert len(taskqueue) == 0


def test_iterator(taskqueue):
    taskqueue.add('bla', LEASE_TIMEOUT, ttl=3)
    taskqueue.add('blip', LEASE_TIMEOUT, ttl=3)

    found_tasks = []
    for task, id in taskqueue:
        found_tasks.append(task)
    assert found_tasks == ['bla', 'blip']


def test_expired_leases_race(taskqueue, monkeypatch, caplog):
    # save the original conn.get so we can use it inside the mock
    get_orig = taskqueue.get_updated_expired_task

    # simulate a race condition in _check_expired_leases where we can
    # still see a task in the set of tasks but by the time we try to get
    # it from the queue it has been completed, i.e. is None
    def mock_get(task_id):
        # marks the task as complete before trying to update the task
        # before trying to update the task
        taskqueue.complete(task_id)
        return get_orig(task_id)

    taskqueue.add('foo', LEASE_TIMEOUT)

    # move task to processing queue
    taskqueue.get()
    time.sleep(LEASE_TIMEOUT + 0.1)

    monkeypatch.setattr(taskqueue, 'get_updated_expired_task', mock_get)
    caplog.set_level(logging.INFO)
    taskqueue._check_expired_leases()
    assert "marked completed while we checked for" in caplog.text


def test_lease_timeout_is_none(taskqueue):
    with pytest.raises(TypeError):
        taskqueue.add('bla', lease_timeout=None)


def test_lease_timeout_is_not_float_or_int(taskqueue):
    # funny thing, a boolean can be converted to float (i.e. 0.0 and
    # 1.0) without causing an error. so be it
    with pytest.raises(ValueError):
        taskqueue.add('bla', lease_timeout="foo")
