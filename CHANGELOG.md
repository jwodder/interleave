v0.3.0 (2025-01-28)
-------------------
- Added `lazy_interleave()` function

v0.2.2 (2024-12-01)
-------------------
- Support Python 3.11, 3.12, and 3.13
- Migrated from setuptools to hatch
- Drop support for Python 3.7

v0.2.1 (2022-07-02)
-------------------
- When an iterator raises an exception, the final traceback will no longer
  include an internal `EndOfInputError`

v0.2.0 (2022-02-20)
-------------------
- Added an `Interleaver.get(block, timeout)` method
- Made the `Interleaver` constructor public and added `submit(iterator)` and
  `finalize()` methods

v0.1.1 (2022-01-22)
-------------------
- Remove `typing-extensions` as a runtime dependency

v0.1.0 (2022-01-17)
-------------------
Initial release
