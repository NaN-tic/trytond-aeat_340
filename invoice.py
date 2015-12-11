# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
from decimal import Decimal
from sql import Literal
from sql.operators import In

from trytond.model import ModelSQL, ModelView, fields
from trytond.wizard import Wizard, StateView, StateTransition, Button
from trytond.pool import Pool, PoolMeta
from trytond.pyson import Eval
from trytond.transaction import Transaction

from .aeat import BOOK_KEY, OPERATION_KEY, PARTY_IDENTIFIER_TYPE

__all__ = ['Type', 'TypeTax', 'TypeTemplateTax',
    'Record', 'AEAT340RecordInvoiceLine',
    'TemplateTax', 'Tax', 'Invoice', 'InvoiceLine',
    'Recalculate340RecordStart', 'Recalculate340RecordEnd',
    'Recalculate340Record', 'Reasign340RecordStart',
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

    @classmethod
    def __register__(cls, module_name):
        pool = Pool()
        ModelData = pool.get('ir.model.data')
        Module = pool.get('ir.module.module')
        cursor = Transaction().cursor
        module_table = Module.__table__()
        sql_table = ModelData.__table__()
        # Meld aeat_340_es into aeat_340
        cursor.execute(*module_table.update(
                columns=[module_table.state],
                values=[Literal('uninstalled')],
                where=module_table.name == Literal('aeat_340_es')
                ))
        cursor.execute(*sql_table.update(
                columns=[sql_table.module],
                values=[module_name],
                where=sql_table.module == Literal('aeat_340_es')))
        super(TypeTemplateTax, cls).__register__(module_name)


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
    book_key = fields.Selection(BOOK_KEY, 'Book Key', sort=False,
        required=True)
    operation_key = fields.Selection(OPERATION_KEY, 'Operation Key',
        sort=False, required=True)
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
    ticket_count = fields.Function(fields.Integer('Ticket Count'),
        'get_ticket_count')
    equivalence_tax_rate = fields.Numeric('Equivalence Tax Rate',
        digits=(16, 2))
    equivalence_tax = fields.Numeric('Equivalence Tax', digits=(16, 2))
    invoice = fields.Many2One('account.invoice', 'Invoice', readonly=True)
    invoice_lines = fields.Many2Many('aeat.340.record-account.invoice.line',
        'aeat340_record', 'invoice_line', 'Invoice Lines')
    issued = fields.Many2One('aeat.340.report.issued', 'Issued',
        readonly=True)
    received = fields.Many2One('aeat.340.report.received', 'Received',
        readonly=True)
    investment = fields.Many2One('aeat.340.report.investment', 'Investment',
        readonly=True)
    intracommunity = fields.Many2One('aeat.340.report.intracommunity',
        'Intracommunity')

    def get_issue_date(self, name):
        return self.invoice.invoice_date

    def get_operation_date(self, name):
        return self.invoice.invoice_date

    def get_invoice_number(self, name):
        return self.invoice.number

    def get_ticket_count(self, name):
        try:
            SaleLine = Pool().get('sale.line')
        except KeyError:
            SaleLine = None
        try:
            PurchaseLine = Pool().get('purchase.line')
        except KeyError:
            PurchaseLine = None
        if self.operation_key == 'B' and record.book_key in ['E', 'F']:
            if SaleLine != None:
                sales = set()
                for inv_line in self.invoice_lines:
                    if isinstance(inv_line.origin, SaleLine):
                        sales.add(inv_line.origin.sale.id)
                return len(sales)
        if self.operation_key == 'B' and record.book_key in ['R', 'S']:
            if PurchaseLine != None:
                purchases = set()
                for inv_line in self.invoice_lines:
                    if isinstance(inv_line.origin, PurchaseLine):
                        purchases.add(inv_line.origin.purchase.id)
                return len(purchases)

    @property
    def corrective_invoice_number(self):
        pool = Pool()
        InvoiceLine = pool.get('account.invoice.line')
        if self.operation_key == 'D':
            for inv_line in self.invoice_lines:
                if isinstance(inv_line.origin, InvoiceLine):
                    return inv_line.origin.invoice.number


class AEAT340RecordInvoiceLine(ModelSQL):
    'AEAT 340 Record - Invoice Line'
    __name__ = 'aeat.340.record-account.invoice.line'
    aeat340_record = fields.Many2One('aeat.340.record', 'AEAT 340 Record',
        ondelete='CASCADE', required=True, select=True)
    invoice_line = fields.Many2One('account.invoice.line', 'Invoice Line',
        ondelete='CASCADE', required=True, select=True)


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

        old_ids = set()
        new_ids = set()
        if tax and len(tax.aeat340_book_keys) > 0:
            old_ids = set([c.id for c in tax.aeat340_book_keys])
        if len(self.aeat340_book_keys) > 0:
            new_ids = set([c.id for c in self.aeat340_book_keys])
            for direction in ('in', 'out'):
                field = "aeat340_default_%s_book_key" % (direction)
                if not tax or getattr(tax, field) != getattr(self, field):
                    value = getattr(self, field)
                    if value and value.id in new_ids:
                        res[field] = value.id
                    else:
                        res[field] = None
        else:
            if tax and tax.aeat340_default_in_book_key:
                res['aeat340_default_in_book_key'] = None
            if tax and tax.aeat340_default_out_book_key:
                res['aeat340_default_out_book_key'] = None

        if old_ids or new_ids:
            key = 'aeat340_book_keys'
            res[key] = []
            to_remove = old_ids - new_ids
            if to_remove:
                res[key].append(['remove', list(to_remove)])
            to_add = new_ids - old_ids
            if to_add:
                res[key].append(['add', list(to_add)])
            if not res[key]:
                del res[key]
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

STATES = {
    'invisible': Eval('type') != 'line',
    }
DEPENDS = ['type']


class InvoiceLine:
    __name__ = 'account.invoice.line'
    aeat340_available_keys = fields.Function(fields.One2Many('aeat.340.type',
        None, 'AEAT 340 Available Keys',
            states=STATES, depends=DEPENDS + ['taxes', 'product']),
        'on_change_with_aeat340_available_keys')
    aeat340_book_key = fields.Many2One('aeat.340.type',
        'AEAT 340 Book Key',
        states=STATES, depends=DEPENDS + ['aeat340_available_keys', 'taxes',
            'invoice_type', 'product'],
        domain=[('id', 'in', Eval('aeat340_available_keys', []))],)
    aeat340_operation_key = fields.Selection(OPERATION_KEY,
        'AEAT 340 Operation Key', states={
            'invisible': Eval('type') != 'line',
            'required': Eval('type') == 'line',
            },
        depends=DEPENDS)

    @fields.depends('invoice', 'taxes')
    def on_change_product(self):
        Taxes = Pool().get('account.tax')

        super(InvoiceLine, self).on_change_product()
        if self.invoice and self.invoice.type:
            type_ = self.invoice.type
        elif self.invoice_type:
            type_ = self.invoice_type

        self.aeat340_book_key = None
        self.aeat340_operation_key = None
        if type_ and self.taxes:
            self.aeat340_book_key = self.get_aeat340_book_key(
                        type_, Taxes.browse(self.taxes))
            self.aeat340_operation_key = self.get_aeat340_operation_key(type_)

    @fields.depends('taxes', 'product')
    def on_change_with_aeat340_available_keys(self, name=None):
        keys = []
        for tax in self.taxes:
            keys.extend([k.id for k in tax.aeat340_book_keys])
            for child_tax in tax.childs:
                keys.extend([k.id for k in child_tax.aeat340_book_keys])
        return list(set(keys))

    @fields.depends('taxes', 'invoice_type', 'aeat340_book_key', 'invoice',
        '_parent_invoice.type', 'product')
    def on_change_with_aeat340_book_key(self):
        if self.aeat340_book_key:
            return self.aeat340_book_key.id

        if self.invoice and self.invoice.type:
            type_ = self.invoice.type
        elif self.invoice_type:
            type_ = self.invoice_type
        else:
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
            if vals.get('type', 'line') != 'line':
                continue
            invoice_type = vals.get('invoice_type')
            if not invoice_type and vals.get('invoice'):
                invoice = Invoice(vals.get('invoice'))
                invoice_type = invoice.type
            if not vals.get('aeat340_book_key') and vals.get('taxes'):
                taxes_ids = []
                for key, value in vals.get('taxes'):
                    if key == 'add':
                        taxes_ids.extend(value)

                vals['aeat340_book_key'] = cls.get_aeat340_book_key(
                    invoice_type, Taxes.browse(taxes_ids))
            if not vals.get('aeat340_operation_key'):
                value = cls.get_aeat340_operation_key(invoice_type)
                vals['aeat340_operation_key'] = value
        return super(InvoiceLine, cls).create(vlist)


class Invoice:
    __name__ = 'account.invoice'

    @property
    def aeat340_record_month(self):
        print "ALLA"
        return self.invoice_date.month

    @classmethod
    def create_aeat340_records(cls, invoices):
        pool = Pool()
        Configuration = pool.get('account.configuration')
        Record = pool.get('aeat.340.record')
        Tax = pool.get('account.tax')

        config = Configuration(1)

        def compute_tax_amount(line, tax):
            context = line.invoice.get_tax_context()
            with Transaction().set_context(**context):
                taxes = Tax.compute([tax], line.unit_price, line.quantity)
                tax_amount = 0
                for tax in taxes:
                    key, val = line.invoice._compute_tax(tax,
                        line.invoice.type)
                    tax_amount += val['amount']
            return tax_amount

        to_create = {}
        for invoice in invoices:
            if not invoice.move or invoice.state == 'cancel':
                continue
            fiscalyear = invoice.move.period.fiscalyear
            party = invoice.party
            for line in invoice.lines:
                if line.type != 'line':
                    continue
                if not line.aeat340_operation_key or not line.aeat340_book_key:
                    continue
                book_key = line.aeat340_book_key.book_key
                operation_key = line.aeat340_operation_key
                base = total = line.amount
                for tax in line.taxes:
                    if not tax.childs:
                        assert not tax.recargo_equivalencia, (
                            "Unexpected recargo_equivalencia flag on "
                            "non-child tax")
                        if line.aeat340_book_key not in tax.aeat340_book_keys:
                            continue
                        tax_rate = tax.rate * 100
                        tax_amount = compute_tax_amount(line, tax)
                        equivalence_tax_amount = equivalence_tax_rate = None
                        total += tax_amount
                    else:
                        tax_rate = equivalence_tax_rate = None
                        tax_amount = equivalence_tax_amount = Decimal(0)
                        for child_tax in tax.childs:
                            child_tax_amount = compute_tax_amount(line,
                                child_tax)
                            total += child_tax_amount
                            if child_tax.recargo_equivalencia:
                                equivalence_tax_rate = child_tax.rate * 100
                                equivalence_tax_amount += child_tax_amount
                            elif (line.aeat340_book_key
                                    in child_tax.aeat340_book_keys):
                                tax_rate = child_tax.rate * 100
                                tax_amount += child_tax_amount
                        if not tax_rate:
                            continue

                    if config.tax_rounding == 'line':
                        base = invoice.currency.round(base)
                        tax_amount = invoice.currency.round(tax_amount)
                        total = invoice.currency.round(total)
                        if equivalence_tax_rate:
                            equivalence_tax_amount = invoice.currency.round(
                                equivalence_tax_amount)

                    key = "%d-%d-%s-%s" % (invoice.id, tax.id,
                        operation_key, book_key)
                    if key in to_create:
                        to_create[key]['base'] += base
                        to_create[key]['tax'] += tax_amount
                        to_create[key]['total'] += total
                        if equivalence_tax_rate:
                            to_create[key]['equivalence_tax'] += (
                                equivalence_tax_amount)
                        to_create[key]['invoice_lines'][0][1].append(line.id)
                    else:
                        to_create[key] = {
                            'company': invoice.company.id,
                            'fiscalyear': fiscalyear,
                            'month': invoice.aeat340_record_month,
                            'party_name': party.rec_name[:40],
                            'party_nif': (party.vat_number[:9]
                                if party.vat_country == 'ES' else ''),
                            'party_country': party.vat_country,
                            'party_identifier_type': '1',
                            'base': base,
                            'tax': tax_amount,
                            'equivalence_tax': (equivalence_tax_amount
                                if equivalence_tax_rate else None),
                            'tax_rate': tax_rate,
                            'equivalence_tax_rate': equivalence_tax_rate,
                            'total': total,
                            'operation_key': operation_key,
                            'book_key': book_key,
                            'invoice': invoice.id,
                            'invoice_lines': [('add', [line.id])],
                            }
            if config.tax_rounding == 'document':
                for key in to_create:
                    if not key.startswith('%d-' % invoice.id):
                        continue
                    to_create[key]['base'] = invoice.currency.round(
                        to_create[key]['base'])
                    to_create[key]['tax_amount'] = invoice.currency.round(
                        to_create[key]['tax_amount'])
                    to_create[key]['total'] = invoice.currency.round(
                        to_create[key]['total'])
                    if to_create[key]['equivalence_tax_rate']:
                        to_create[key]['equivalence_tax_rate'] = (
                            invoice.currency.round(
                                to_create[key]['equivalence_tax_rate']))

        with Transaction().set_user(0, set_context=True):
            Record.delete(Record.search([('invoice', 'in',
                            [i.id for i in invoices])]))
            Record.create(to_create.values())

    @classmethod
    def draft(cls, invoices):
        pool = Pool()
        Record = pool.get('aeat.340.record')
        super(Invoice, cls).draft(invoices)
        with Transaction().set_user(0, set_context=True):
            Record.delete(Record.search([('invoice', 'in',
                            [i.id for i in invoices])]))

    @classmethod
    def post(cls, invoices):
        super(Invoice, cls).post(invoices)
        cls.create_aeat340_records(invoices)

    @classmethod
    def cancel(cls, invoices):
        pool = Pool()
        Record = pool.get('aeat.340.record')
        super(Invoice, cls).cancel(invoices)
        with Transaction().set_user(0, set_context=True):
            Record.delete(Record.search([('invoice', 'in',
                            [i.id for i in invoices])]))


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

    @classmethod
    def __setup__(cls):
        super(Reasign340Record, cls).__setup__()
        cls._error_messages.update({
                'aeat340_book_key_not_available': (
                    'The AEAT 340 Book Key "%s" is not available for any of '
                    'selected invoices.'),
                })

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
            if not lines:
                self.raise_user_error('aeat340_book_key_not_available',
                    value.rec_name)

            # Update to allow to modify key for posted invoices
            cursor.execute(*line.update(columns=[line.aeat340_book_key],
                    values=[value.id], where=In(line.id, lines)))
        value = self.start.operation_key
        if value:
            lines = []
            for invoice in invoices:
                for l in invoice.lines:
                    lines.append(l.id)

            # Update to allow to modify key for posted invoices
            cursor.execute(*line.update(columns=[line.aeat340_operation_key],
                    values=[value], where=In(line.id, lines)))

        invoices = Invoice.browse(invoices)
        Invoice.create_aeat340_records(invoices)

        return 'done'
