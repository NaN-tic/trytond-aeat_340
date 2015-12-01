# This file is part of the aeat_340 module for Tryton.
# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
import unittest
import trytond.tests.test_tryton
from trytond.tests.test_tryton import ModuleTestCase


class Aeat340TestCase(ModuleTestCase):
    'Test Aeat 340 module'
    module = 'aeat_340'


def suite():
    suite = trytond.tests.test_tryton.suite()
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(
        Aeat340TestCase))
    return suite