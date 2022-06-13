.. image:: http://www.repostatus.org/badges/latest/active.svg
    :target: http://www.repostatus.org/#active
    :alt: Project Status: Active — The project has reached a stable, usable
          state and is being actively developed.

.. image:: https://github.com/jwodder/interleave/workflows/Test/badge.svg?branch=master
    :target: https://github.com/jwodder/interleave/actions?workflow=Test
    :alt: CI Status

.. image:: https://codecov.io/gh/jwodder/interleave/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/jwodder/interleave

.. image:: https://img.shields.io/pypi/pyversions/interleave.svg
    :target: https://pypi.org/project/interleave/

.. image:: https://img.shields.io/conda/vn/conda-forge/interleave.svg
    :target: https://anaconda.org/conda-forge/interleave
    :alt: Conda Version

.. image:: https://img.shields.io/github/license/jwodder/interleave.svg
    :target: https://opensource.org/licenses/MIT
    :alt: MIT License

`GitHub <https://github.com/jwodder/interleave>`_
| `PyPI <https://pypi.org/project/interleave/>`_
| `Issues <https://github.com/jwodder/interleave/issues>`_
| `Changelog <https://github.com/jwodder/interleave/blob/master/CHANGELOG.md>`_

The ``interleave`` package provides a function of the same name that takes a
number of iterators, runs them in separate threads, and yields the values
produced as soon as each one is available.

Installation
============
``interleave`` requires Python 3.7 or higher.  Just use `pip
<https://pip.pypa.io>`_ for Python 3 (You have pip, right?) to install
``interleave`` and its dependencies::

    python3 -m pip install interleave


Example
=======

>>> from time import sleep, strftime
>>> from interleave import interleave
>>>
>>> def sleeper(idno, delays):
...     for i, d in enumerate(delays):
...         sleep(d)
...         yield (idno, i)
...
>>> with interleave(
...     [
...         sleeper(0, [0, 1, 2]),
...         sleeper(1, [2, 2, 2]),
...         sleeper(2, [5, 2, 1]),
...     ]
... ) as it:
...     for x in it:
...         print(strftime("%H:%M:%S"), x)
...
22:08:39 (0, 0)
22:08:40 (0, 1)
22:08:41 (1, 0)
22:08:42 (0, 2)
22:08:43 (1, 1)
22:08:44 (2, 0)
22:08:45 (1, 2)
22:08:46 (2, 1)
22:08:47 (2, 2)


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
    ) -> interleave.Interleaver[T]

``interleave()`` runs the given iterators in separate threads and returns an
iterator that yields the values yielded by them as they become available.  (See
below for details on the ``Interleaver`` class.)

The ``max_workers`` and ``thread_name_prefix`` parameters are passed through to
the underlying |ThreadPoolExecutor|_ (q.v.).  ``max_workers`` determines the
maximum number of iterators to run at one time.

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
    iterators that haven't been started yet, wait for all running threads to
    finish, and reraise the exception.  Note that, due to the inability to stop
    an iterator between yields, the "waiting" step involves waiting for each
    currently-running iterator to yield its next value before stopping.  This
    can deadlock if the queue fills up in the interim.

``DRAIN``
    Like ``STOP``, but any remaining values yielded by the iterators before
    they finish are yielded by the interleaver before raising the exception

``FINISH_ALL``
    Continue running as normal and reraise the exception once all iterators
    have finished

``FINISH_CURRENT``
    Like ``FINISH_ALL``, except that only currently-running iterators are run
    to completion; any iterators whose threads haven't yet been started when
    the exception is raised will have their jobs cancelled

Regardless of the value of ``onerror``, any later exceptions raised by
iterators after the initial exception are discarded.

.. code:: python

    class Interleaver(Generic[T]):
        def __init__(
            self,
            max_workers: Optional[int] = None,
            thread_name_prefix: str = "",
            queue_size: Optional[int] = None,
            onerror: OnError = STOP,
        )

An iterator and context manager.  As an iterator, it yields the values
generated by the iterators passed to the corresponding ``interleave()`` call as
they become available.  As a context manager, it returns itself on entry and,
on exit, cleans up any unfinished threads by calling the
``shutdown(wait=True)`` method (see below).

An ``Interleaver`` can be instantiated either by calling ``interleave()`` or by
calling the constructor directly.  The constructor takes the same arguments as
``interleave()``, minus ``iterators``, and produces a new ``Interleaver`` that
is not yet running any iterators.  Iterators are submitted to a new
``Interleaver`` via the ``submit()`` method; once all desired iterators have
been submitted, the ``finalize()`` method **must** be called so that the
``Interleaver`` can tell when everything's finished.

An ``Interleaver`` will shut down its ``ThreadPoolExecutor`` and wait for the
threads to finish after yielding its final value (specifically, when a call is
made to ``__next__``/``get()`` that would result in ``StopIteration`` or
another exception being raised).  In the event that an ``Interleaver`` is
abandoned before iteration completes, the associated resources may not be
properly cleaned up, and threads may continue running indefinitely.  For this
reason, it is strongly recommended that you wrap any iteration over an
``Interleaver`` in the context manager in order to handle a premature end to
iteration (including from a ``KeyboardInterrupt``).

Besides the iterator and context manager APIs, an ``Interleaver`` has the
following public methods:

.. code:: python

    Interleaver.submit(it: Iterator[T]) -> None

*New in version 0.2.0*

Add an iterator to the ``Interleaver``.

If the ``Interleaver`` was returned from ``interleave()`` or has already had
``finalize()`` called on it, calling ``submit()`` will result in a
``ValueError``.

.. code:: python

    Interleave.finalize() -> None

*New in version 0.2.0*

Notify the ``Interleaver`` that all iterators have been registered.  This
method must be called in order for the ``Interleaver`` to detect the end of
iteration; if this method has not been called and all submitted iterators have
finished & had their values retrieved, then a subsequent call to ``next(it)``
will end up hanging indefinitely.

.. code:: python

    Interleaver.get(block: bool = True, timeout: Optional[float] = None) -> T

*New in version 0.2.0*

Fetch the next value generated by the iterators.  If all iterators have
finished and all values have been retrieved, raises
``interleaver.EndOfInputError``.  If ``block`` is ``False`` and no values are
immediately available, raises ``queue.Empty``.  If ``block`` is ``True``, waits
up to ``timeout`` seconds (or indefinitely, if ``timeout`` is ``None``) for the
next value to become available or for all iterators to end; if nothing happens
before the timeout expires, raises ``queue.Empty``.

``it.get(block=True, timeout=None)`` is equivalent to ``next(it)``, except that
the latter converts an ``EndOfInputError`` to ``StopIteration``.

**Note:** When ``onerror=STOP`` and a timeout is set, if an iterator raises an
exception, the timeout may be exceeded as the ``Interleaver`` waits for all
remaining threads to shut down.

.. code:: python

    Interleaver.shutdown(wait: bool = True) -> None

Call ``finalize()`` if it hasn't been called yet, tell all running iterators to
stop iterating, cancel any outstanding iterators that haven't been started yet,
and shut down the ``ThreadPoolExecutor``.  The ``wait`` parameter is passed
through to the call to ``ThreadPoolExecutor.shutdown()``.

The ``Interleaver`` can continue to be iterated over after calling
``shutdown()`` and will yield any remaining values produced by the iterators
before they stopped completely.
