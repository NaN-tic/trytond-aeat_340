from trytond.model import ModelSQL, ModelView, fields
from trytond.wizard import Wizard, StateView, StateTransition, Button
from trytond.pool import Pool, PoolMeta
from trytond.pyson import Eval
from trytond.transaction import Transaction
from sql.operators import In
from .aeat import BOOK_KEY, OPERATION_KEY, PARTY_IDENTIFIER_TYPE

__all__ = ['Type', 'TypeTax', 'TypeTemplateTax', 'Record', 'TemplateTax',
    'Tax', 'Invoice', 'InvoiceLine', 'Recalculate340RecordStart',
    'Recalculate340RecordEnd', 'Recalculate340Record', 'Reasign340RecordStart',
    'Reasign340RecordEnd', 'Reasign340Record']

__metaclass__ = PoolMeta


class Type(ModelSQL, ModelView):
    """
    AEAT 340 Type

    Keys types for AEAT 340 Report
    """
    __name__ = 'aeat.340.type'
    _rec_name = 'book_key'

    book_key = fields.Selection(BOOK_KEY, 'Book key',
        required=True)

    @classmethod
    def __setup__(cls):
        super(Type, cls).__setup__()
        cls._sql_constraints += [
            ('book_key_uniq', 'unique (book_key)',
                'unique_book_key')
            ]
        cls._error_messages.update({
                'unique_book_key': 'Book key must be unique.',
                })


class TypeTax(ModelSQL):
    """
    AEAT 340 Type-Tax Relation
    """
    __name__ = 'aeat.340.type-account.tax'

    aeat_340_type = fields.Many2One('aeat.340.type', 'Book Key',
        ondelete='CASCADE', select=True, required=True)
    tax = fields.Many2One('account.tax', 'Tax', ondelete='CASCADE',
        select=True, required=True)


class TypeTemplateTax(ModelSQL):
    """
    AEAT 340 Type-Template Tax Relation
    """
    __name__ = 'aeat.340.type-account.tax.template'

    aeat_340_type = fields.Many2One('aeat.340.type', 'Book Key',
        ondelete='CASCADE', select=True, required=True)
    tax = fields.Many2One('account.tax.template', 'Template Tax',
        ondelete='CASCADE', select=True, required=True)


class Record(ModelSQL, ModelView):
    """
    AEAT 340 Record

    Calculated on invoice creation to generate temporal
    data for reports. Aggregated on aeat340 calculation.
    """
    __name__ = 'aeat.340.record'

    company = fields.Many2One('company.company', 'Company', required=True,
        readonly=True)
    fiscalyear = fields.Many2One('account.fiscalyear', 'Fiscal Year',
        required=True, readonly=True)
    month = fields.Integer('Month', readonly=True)
    party_nif = fields.Char('Party CIF/NIF', size=9)
    party_name = fields.Char('Party Name', size=40)
    party_country = fields.Char('Party Country', size=2)
    party_identifier_type = fields.Selection(PARTY_IDENTIFIER_TYPE,
        'Party Identifier Type', required=True)
    party_identifier = fields.Char('Party Identifier', size=20)
    book_key = fields.Selection(BOOK_KEY, 'Book Key', sort=False, required=True)
    operation_key = fields.Selection(OPERATION_KEY, 'Operation Key', sort=False,
        required=True)
    issue_date = fields.Function(fields.Date('Issue Date', required=True),
        'get_issue_date')
    operation_date = fields.Function(fields.Date('Operation Date',
            required=True), 'get_operation_date')
    tax_rate = fields.Numeric('Tax Rate', digits=(16, 2), required=True)
    base = fields.Numeric('Base', digits=(16, 2), required=True)
    tax = fields.Numeric('Tax', digits=(16, 2), required=True)
    total = fields.Numeric('Total', digits=(16, 2), required=True)
    invoice_number = fields.Function(fields.Char('Invoice Number', size=40),
        'get_invoice_number')
    invoice = fields.Many2One('account.invoice', 'Invoice', readonly=True)
    issued = fields.Many2One('aeat.340.report.issued', 'Issued',
        readonly=True)
    received = fields.Many2One('aeat.340.report.received', 'Received',
        readonly=True)
    investment = fields.Many2One('aeat.340.report.investment', 'Investment',
        readonly=True)
    intracommunity = fields.Many2One('aeat.340.report.intracommunity',
        'Intracommunity', readonly=True)

    def get_issue_date(self, name=None):
        return self.invoice.invoice_date

    def get_operation_date(self, name=None):
        return self.invoice.invoice_date

    def get_invoice_number(self, name=None):
        return self.invoice.rec_name


class TemplateTax:
    __name__ = 'account.tax.template'

    aeat340_book_keys = fields.Many2Many('aeat.340.type-account.tax.template',
        'tax', 'aeat_340_type', 'Available Book Keys')
    aeat340_default_out_book_key = fields.Many2One('aeat.340.type',
        'Default Out Book Key',
        domain=[('id', 'in', Eval('aeat340_book_keys', []))],
        depends=['aeat340_book_keys'])
    aeat340_default_in_book_key = fields.Many2One('aeat.340.type',
        'Default In Book Key',
        domain=[('id', 'in', Eval('aeat340_book_keys', []))],
        depends=['aeat340_book_keys'])

    def _get_tax_value(self, tax=None):
        res = super(TemplateTax, self)._get_tax_value(tax)

        res['aeat340_book_keys'] = []
        if tax and len(tax.aeat_340_book_keys) > 0:
            res['aeat340_book_keys'].append(['unlink_all'])
        if len(self.aeat340_book_keys) > 0:
            ids = [c.id for c in self.aeat340_book_keys]
            res['aeat340_book_keys'].append(['set', ids])
        for direction in ('in', 'out'):
            field = "aeat340_default_%s_book_key" % (direction)
            if not tax or getattr(tax, field) != getattr(self, field):
                value = getattr(self, field)
                if value:
                    res[field] = getattr(self, field).id
                else:
                    res[field] = None
        return res


class Tax:
    __name__ = 'account.tax'

    aeat340_book_keys = fields.Many2Many('aeat.340.type-account.tax',
        'tax', 'aeat_340_type', 'Available Book Keys')
    aeat340_default_out_book_key = fields.Many2One('aeat.340.type',
        'Default Out Book Key',
        domain=[('id', 'in', Eval('aeat340_book_keys', []))],
        depends=['aeat340_book_keys'])
    aeat340_default_in_book_key = fields.Many2One('aeat.340.type',
        'Default In Book Key',
        domain=[('id', 'in', Eval('aeat340_book_keys', []))],
        depends=['aeat340_book_keys'])


class InvoiceLine:
    __name__ = 'account.invoice.line'
    aeat340_available_keys = fields.Function(fields.One2Many('aeat.340.type',
        None, 'AEAT 340 Available Keys', on_change_with=['taxes', 'product'],
        depends=['taxes', 'product']), 'on_change_with_aeat340_available_keys')
    aeat340_book_key = fields.Many2One('aeat.340.type',
        'AEAT 340 Book Key', on_change_with=['taxes', 'invoice_type',
            'aeat340_book_key', '_parent_invoice.type', 'product'],
        depends=['aeat340_available_keys', 'taxes', 'invoice_type', 'product'],
        domain=[('id', 'in', Eval('aeat340_available_keys', []))],)
    aeat340_operation_key = fields.Selection(OPERATION_KEY,
        'AEAT 340 Operation Key', required=True)

    def on_change_product(self):
        Taxes = Pool().get('account.tax')
        res = super(InvoiceLine, self).on_change_product()
        if self.invoice and self.invoice.type:
            type_ = self.invoice.type
        elif self.invoice_type:
            type_ = self.invoice_type
        if 'taxes' in res:
            res['aeat340_book_key'] = self.get_aeat340_book_key(
                        type_, Taxes.browse(res['taxes']))
        value = self.get_aeat340_operation_key(type_)
        res['aeat340_operation_key'] = value
        return res

    def on_change_with_aeat340_available_keys(self, name=None):
        keys = []
        for tax in self.taxes:
            keys.extend([k.id for k in tax.aeat340_book_keys])
        return list(set(keys))

    def on_change_with_aeat340_book_key(self):
        if self.aeat340_book_key:
            return self.aeat340_book_key.id

        if self.invoice and self.invoice.type:
            type_ = self.invoice.type
        elif self.invoice_type:
            type_ = self.invoice_type
        if not type_:
            return

        return self.get_aeat340_book_key(type_, self.taxes)

    @classmethod
    def get_aeat340_book_key(cls, invoice_type, taxes):
        type_ = 'in' if invoice_type[0:2] == 'in' else 'out'
        for tax in taxes:
            name = 'aeat340_default_%s_book_key' % type_
            value = getattr(tax, name)
            if value:
                return value.id

    @classmethod
    def get_aeat340_operation_key(cls, invoice_type):
        return 'D' if 'credit_note' in invoice_type else 'C'

    @classmethod
    def create(cls, vlist):
        Invoice = Pool().get('account.invoice')
        Taxes = Pool().get('account.tax')
        for vals in vlist:
            invoice_type = vals.get('invoice_type')
            if not invoice_type and vals.get('invoice'):
                invoice = Invoice(vals.get('invoice'))
                invoice_type = invoice.type
            if not vals.get('aeat340_book_key') and vals.get('taxes'):
                taxes_ids = []
                for key, value in vals.get('taxes'):
                    if key in ['add', 'set']:
                        taxes_ids.extend(value)

                vals['aeat340_book_key'] = cls.get_aeat340_book_key(
                    invoice_type, Taxes.browse(taxes_ids))
            if not vals.get('aeat340_operation_key'):
                value = cls.get_aeat340_operation_key(invoice_type)
                vals['aeat340_operation_key'] = value
        return super(InvoiceLine, cls).create(vlist)


class Invoice:
    __name__ = 'account.invoice'

    @classmethod
    def create_aeat340_records(cls, invoices):
        Record = Pool().get('aeat.340.record')
        to_create = {}
        for invoice in invoices:
            if not invoice.move:
                continue
            fiscalyear = invoice.move.period.fiscalyear
            party = invoice.party
            for line in invoice.lines:
                if line.aeat340_operation_key and line.aeat340_book_key:
                        book_key = line.aeat340_book_key.book_key
                        operation_key = line.aeat340_operation_key
                        for tax in line.invoice_taxes:
#TODO: Validar que l'impost tingui claus disponibles (per evitar 3 linies)
                            key = "%d-%d-%s-%s" % (invoice.id, tax.id,
                                operation_key, book_key)
                            total = (tax.amount + tax.base)
                            if key in to_create:
                                to_create[key]['base'] += tax.base
                                to_create[key]['tax'] += tax.amount
                                to_create[key]['total'] += total
                            else:
                                to_create[key] = {
                                        'company': invoice.company.id,
                                        'fiscalyear': fiscalyear,
                                        'month': invoice.invoice_date.month,
                                        'party_name': party.rec_name,
                                        'party_nif': party.vat_code,
                                        'party_country': party.vat_country,
                                        'party_identifier_type': '1',
                                        'base': tax.base,
                                        'tax': tax.amount,
                                        'tax_rate': tax.tax.rate * 100,
                                        'total': total,
                                        'operation_key': operation_key,
                                        'book_key': book_key,
                                        'invoice': invoice.id,
                                }
        with Transaction().set_user(0, set_context=True):
            Record.delete(Record.search([('invoice', 'in',
                            [i.id for i in invoices])]))
            Record.create(to_create.values())

    @classmethod
    def post(cls, invoices):
        super(Invoice, cls).post(invoices)
        cls.create_aeat340_records(invoices)


class Recalculate340RecordStart(ModelView):
    """
    Recalculate AEAT 340 Records Start
    """
    __name__ = "aeat.340.recalculate.records.start"


class Recalculate340RecordEnd(ModelView):
    """
    Recalculate AEAT 340 Records End
    """
    __name__ = "aeat.340.recalculate.records.end"


class Recalculate340Record(Wizard):
    """
    Recalculate AEAT 340 Records
    """
    __name__ = "aeat.340.recalculate.records"
    start = StateView('aeat.340.recalculate.records.start',
        'aeat_340.aeat_340_recalculate_start_view', [
            Button('Cancel', 'end', 'tryton-cancel'),
            Button('Calculate', 'calculate', 'tryton-ok', default=True),
            ])
    calculate = StateTransition()
    done = StateView('aeat.340.recalculate.records.end',
        'aeat_340.aeat_340_recalculate_end_view', [
            Button('Ok', 'end', 'tryton-ok', default=True),
            ])

    def transition_calculate(self):
        Invoice = Pool().get('account.invoice')
        invoices = Invoice.browse(Transaction().context['active_ids'])
        Invoice.create_aeat340_records(invoices)
        return 'done'


class Reasign340RecordStart(ModelView):
    """
    Reasign AEAT 340 Records Start
    """
    __name__ = "aeat.340.reasign.records.start"

    aeat_340_type = fields.Many2One('aeat.340.type', 'Book Key')
    operation_key = fields.Selection(OPERATION_KEY, 'Operation Key')


class Reasign340RecordEnd(ModelView):
    """
    Reasign AEAT 340 Records End
    """
    __name__ = "aeat.340.reasign.records.end"


class Reasign340Record(Wizard):
    """
    Reasign AEAT 340 Records
    """
    __name__ = "aeat.340.reasign.records"
    start = StateView('aeat.340.reasign.records.start',
        'aeat_340.aeat_340_reasign_start_view', [
            Button('Cancel', 'end', 'tryton-cancel'),
            Button('Reasign', 'reasign', 'tryton-ok', default=True),
            ])
    reasign = StateTransition()
    done = StateView('aeat.340.reasign.records.end',
        'aeat_340.aeat_340_reasign_end_view', [
            Button('Ok', 'end', 'tryton-ok', default=True),
            ])

    def transition_reasign(self):
        Invoice = Pool().get('account.invoice')
        Line = Pool().get('account.invoice.line')
        cursor = Transaction().cursor
        invoices = Invoice.browse(Transaction().context['active_ids'])

        line = Line.__table__()
        value = self.start.aeat_340_type
        if value:
            lines = []
            for invoice in invoices:
                for l in invoice.lines:
                    if value in l.aeat340_available_keys:
                        lines.append(l.id)

            #Update to allow to modify key for posted invoices
            cursor.execute(*line.update(columns=[line.aeat340_book_key],
                    values=[value.id], where=In(line.id, lines)))
        value = self.start.operation_key
        if value:
            lines = []
            for invoice in invoices:
                for l in invoice.lines:
                    lines.append(l.id)

            #Update to allow to modify key for posted invoices
            cursor.execute(*line.update(columns=[line.aeat340_operation_key],
                    values=[value], where=In(line.id, lines)))

        invoices = Invoice.browse(invoices)
        Invoice.create_aeat340_records(invoices)

        return 'done'
