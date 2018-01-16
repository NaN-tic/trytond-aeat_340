# This file is part of Tryton.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.
from trytond.pool import Pool
from . import aeat
from . import invoice


def register():
    Pool.register(
        aeat.Report,
        aeat.Issued,
        aeat.Received,
        aeat.Investment,
        aeat.Intracommunity,
        invoice.Type,
        invoice.TypeTax,
        invoice.TypeTemplateTax,
        invoice.Record,
        invoice.AEAT340RecordInvoiceLine,
        invoice.TemplateTax,
        invoice.Tax,
        invoice.Invoice,
        invoice.InvoiceLine,
        invoice.Recalculate340RecordStart,
        invoice.Recalculate340RecordEnd,
        invoice.Reasign340RecordStart,
        invoice.Reasign340RecordEnd,
        module='aeat_340', type_='model')
    Pool.register(
        invoice.Recalculate340Record,
        invoice.Reasign340Record,
        module='aeat_340', type_='wizard')
