#This file is part of Tryton.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
from trytond.pool import Pool
from .aeat import *
from .invoice import *


def register():
    Pool.register(
        Report,
        Issued,
        Received,
        Investment,
        Intracommunity,
        Type,
        TypeTax,
        TypeTemplateTax,
        Record,
        TemplateTax,
        Tax,
        Invoice,
        InvoiceLine,
        Recalculate340RecordStart,
        Recalculate340RecordEnd,
        Reasign340RecordStart,
        Reasign340RecordEnd,
        module='aeat_340', type_='model')
    Pool.register(
        Recalculate340Record,
        Reasign340Record,
        module='aeat_340', type_='wizard')
