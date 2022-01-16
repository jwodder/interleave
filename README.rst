.. image:: http://www.repostatus.org/badges/latest/wip.svg
    :target: http://www.repostatus.org/#wip
    :alt: Project Status: WIP — Initial development is in progress, but there
          has not yet been a stable, usable release suitable for the public.

.. image:: https://github.com/jwodder/interleave/workflows/Test/badge.svg?branch=master
    :target: https://github.com/jwodder/interleave/actions?workflow=Test
    :alt: CI Status

.. image:: https://codecov.io/gh/jwodder/interleave/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/jwodder/interleave

.. image:: https://img.shields.io/github/license/jwodder/interleave.svg
    :target: https://opensource.org/licenses/MIT
    :alt: MIT License

`GitHub <https://github.com/jwodder/interleave>`_
| `Issues <https://github.com/jwodder/interleave/issues>`_

The ``interleave`` package provides a function of the same name that takes a
number of iterators, runs them in separate threads, and yields the values
produced as soon as each one is available.  It is currently a work in progress.

Installation
============
``interleave`` requires Python 3.7 or higher.  Just use `pip
<https://pip.pypa.io>`_ for Python 3 (You have pip, right?) to install
``interleave`` and its dependencies::

    python3 -m pip install git+https://github.com/jwodder/interleave.git


Example
=======

>>> from time import sleep
>>> from interleave import interleave
>>>
>>> def sleeper(idno, delays):
...     for i, d in enumerate(delays):
...         sleep(d)
...         yield (idno, i)
...
>>> for x in interleave(
...     [
...         sleeper(0, [0, 1, 2]),
...         sleeper(1, [2, 2, 2]),
...         sleeper(2, [5, 2, 1]),
...     ]
... ):
...     print(x)
...
(0, 0)
(0, 1)
(1, 0)
(0, 2)
(1, 1)
(2, 0)
(1, 2)
(2, 1)
(2, 2)


API
===

.. code:: python

    interleave.interleave(
        iterators: Iterable[Iterator[T]],
        *,
        max_workers: Optional[int] = None,
        thread_name_prefix: str = "",
        queue_size: Optional[int] = None,
        onerror: interleave.OnError = interleave.STOP,
    ) -> Iterator[T]

``interleave()`` runs the given iterators in separate threads and yields the
values yielded by them as they become available.

The ``max_workers`` and ``thread_name_prefix`` parameters are passed through to
the underlying |ThreadPoolExecutor|_ (q.v.).  ``max_workers`` determines the
maximum number of iterators to run at once.

.. |ThreadPoolExecutor| replace:: ``concurrent.futures.ThreadPoolExecutor``
.. _ThreadPoolExecutor:
   https://docs.python.org/3/library/concurrent.futures.html
   #concurrent.futures.ThreadPoolExecutor

The ``queue_size`` parameter sets the maximum size of the queue used internally
to pipe values yielded by the iterators; when the queue is full, any iterator
with a value to yield will block waiting for the next value to be dequeued by a
call to the interleaver's ``__next__``.  When ``queue_size`` is ``None`` (the
default), ``interleave()`` uses a ``queue.SimpleQueue``, which has no maximum
size.  When ``queue_size`` is non-``None`` (including zero, signifying no
maximum size), ``interleave()`` uses a ``queue.Queue``, whose ``get()`` method
is uninterruptible (including by ``KeyboardInterrupt``) on Windows.

The ``onerror`` parameter is an enum that determines how ``interleave()``
should behave if one of the iterators raises an exception.  The possible values
are:

``STOP``
    *(default)* Stop iterating over all iterators, cancel any outstanding
    threads, wait for all running threads to finish, and reraise the exception.
    Note that, due to the inability to stop an iterator between yields, the
    "waiting" step involves waiting for each currently-running iterator to
    yield its next value before stopping.  This can deadlock if the queue fills
    up in the interim.

``DRAIN``
    Like ``STOP``, but any remaining values yielded by the iterators before
    they finish are yielded by the interleaver before raising the exception

``FINISH_ALL``
    Continue running as normal and reraise the exception once all iterators
    have finished

``FINISH_CURRENT``
    Like ``FINISH_ALL``, except that only currently-running iterators are run
    to completion; any iterators whose threads haven't yet been started when
    the exception is raised will have their threads cancelled

Regardless of the value of ``onerror``, any later exceptions raised by
iterators after the initial exception are discarded.
