# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
try:
    from trytond.modules.aeat_340.tests.test_aeat_340 import suite
except ImportError:
    from .test_aeat_340 import suite

__all__ = ['suite']
