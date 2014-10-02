try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

try:
    from unittest import mock
except ImportError:
    import mock  # noqa
