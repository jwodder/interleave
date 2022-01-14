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
