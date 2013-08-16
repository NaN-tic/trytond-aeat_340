#This file is part of Tryton.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
from trytond.pool import Pool
from .aeat import *


def register():
    Pool.register(
        Report,
        Issued,
        Received,
        Investment,
        Intracommunity,
        module='aeat_340', type_='model')
