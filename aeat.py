# -*- coding: utf-8 -*-
# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
import itertools
import datetime
import retrofix
import unicodedata
from decimal import Decimal
from retrofix import aeat340
from sql import Null

from trytond import backend
from trytond.model import ModelSQL, ModelView, fields, Workflow
from trytond.pyson import Eval
from trytond.pool import Pool
from trytond.transaction import Transaction

__all__ = ['Report', 'Issued', 'Received', 'Investment', 'Intracommunity']

_ZERO = Decimal('0.0')

BOOK_KEY = [
    ('E', 'Issued Invoices'),
    ('I', 'Investment Goods'),
    ('R', 'Received Invoices'),
    ('U', 'Particular Intracommunity Operations'),
    ('F', 'IGIC Issued Invoices'),
    ('J', 'IGIC Investment Goods'),
    ('S', 'IGIC Received Invoices'),
    ]

OPERATION_KEY = [
    (' ', 'Normal operation'),
    ('A', 'A - Invoice\'s Summary'),
    ('B', 'B - Ticket\'s Summary'),
    ('C', 'C - Invoice with several taxes'),
    ('D', 'D - Credit Note'),
    # ('E', '(optional)'),
    ('F', 'F - Travel agencies acquisitions'),
    ('G', 'G - Special arrangment parties on IVA or IGIC'),
    ('H', 'H - Gold inversion special arrangment'),
    ('I', 'I - Passive subject inversion'),
    ('J', 'J - Tickets'),
    ('K', 'K - Rectification of registry mistakes'),
    ('L', 'L - Acquisitions to retailers of IGIC'),
    # ('M', '(optional)'),
    ('N', 'N - Services to travel agencies'),
    # ('O', '(optional)'),
    # ('P', '(optional)'),
    # ('Q', '(optional)'),
    ('R', 'R - Operations of lease of bussiness place'),
    ('S', 'S - Grants, aids and subsidies'),
    ('T', 'T - Intelectual properties charges'),
    ('U', 'U - Insurance Operations'),
    ('V', 'V - Travel angencies buys'),
    ('W', 'W - Operations subject to taxes of Ceuta and Melilla'),
    ('X', 'X - Agricultural or farming compensations'),
    ('', 'None of the Above'),
    ]

PARTY_IDENTIFIER_TYPE = [
    ('1', 'NIF'),
    ('2', 'NIF (Intracommunitary Operator)'),
    ('3', 'Passport'),
    ('4', 'Official Document Emmited by the Country of Residence'),
    ('5', 'Certificate of fiscal resident'),
    ('6', 'Other proving document'),
    ]

PROPERTY_STATE = [
    ('0', ''),
    ('1',
        '1. Property with cadastral reference located at any point in the '
        'Spanish territory, except the Basque Country and Navarra.'),
    ('2',
        '2. Property located in the Autonomous Community of the Basque '
        'Country or in the Comunidad Foral de Navarra.'),
    ('3',
        '3. Property in any of the above situations but without cadastral '
        'reference.'),
    ('4', '4. Property located in the foreign country.'),
    ]


def remove_accents(unicode_string):
    if isinstance(unicode_string, str):
        unicode_string_bak = unicode_string
        try:
            unicode_string = unicode_string_bak.decode('iso-8859-1')
        except UnicodeDecodeError:
            try:
                unicode_string = unicode_string_bak.decode('utf-8')
            except UnicodeDecodeError:
                return unicode_string_bak

    if not isinstance(unicode_string, unicode):
        return unicode_string

    # From http://www.leccionespracticas.com/uncategorized/eliminar-tildes-con-python-solucionado
    unicode_string_nfd = ''.join(
        (c for c in unicodedata.normalize('NFD', unicode_string)
            if (unicodedata.category(c) != 'Mn'
                or c in (u'\u0327', u'\u0303'))  # ç or ñ
            ))
    # It converts nfd to nfc to allow unicode.decode()
    return unicodedata.normalize('NFC', unicode_string_nfd)

_STATES = {
    'readonly': Eval('state') != 'draft',
    }
_DEPENDS = ['state']

class Report(Workflow, ModelSQL, ModelView):
    '''
    AEAT 340 Report
    '''
    __name__ = 'aeat.340.report'
    company = fields.Many2One('company.company', 'Company', required=True,
        states={
            'readonly': Eval('state') != 'draft',
            }, depends=['state'])
    currency = fields.Function(fields.Many2One('currency.currency',
        'Currency'), 'get_currency')
    previous_number = fields.Char('Previous Declaration Number', size=13,
        states={
            'readonly': ~Eval('state').in_(['draft', 'calculated']),
            }, depends=['state'])
    representative_vat = fields.Char('L.R. VAT number', size=9,
        help='Legal Representative VAT number.', states={
            'required': Eval('state').in_(['calculated', 'done']),
            'readonly': ~Eval('state').in_(['draft', 'calculated']),
            }, depends=['state'])
    fiscalyear = fields.Many2One('account.fiscalyear', 'Fiscal Year',
        required=True, states={
            'readonly': Eval('state') != 'draft',
            }, depends=['state'])
    fiscalyear_code = fields.Integer('Fiscal Year Code', required=True,
        states={
            'readonly': Eval('state') != 'draft',
            }, depends=['state'])
    company_vat = fields.Char('VAT', size=9, states={
            'required': Eval('state').in_(['calculated', 'done']),
            'readonly': ~Eval('state').in_(['draft', 'calculated']),
            }, depends=['state'])
    type = fields.Selection([
            ('N', 'Normal'),
            ('C', 'Complementary'),
            ('S', 'Substitutive')
            ], 'Statement Type', required=True,
        states={
            'readonly': ~Eval('state').in_(['draft', 'calculated']),
            }, depends=['state'])
    support_type = fields.Selection([
            ('C', 'DVD'),
            ('T', 'Telematics'),
            ], 'Support Type', required=True,
        states={
            'readonly': ~Eval('state').in_(['draft', 'calculated']),
            }, depends=['state'])
    calculation_date = fields.DateTime('Calculation Date', readonly=True)
    contact_phone = fields.Char('Phone', size=9,
        states={
            'readonly': ~Eval('state').in_(['draft', 'calculated']),
            }, depends=['state'])
    contact_name = fields.Char('Name And Surname Contact', size=40,
        states={
            'readonly': ~Eval('state').in_(['draft', 'calculated']),
            }, depends=['state'])
    period = fields.Selection([
            ('1T', 'First quarter'),
            ('2T', 'Second quarter'),
            ('3T', 'Third quarter'),
            ('4T', 'Fourth quarter'),
            ('01', 'January'),
            ('02', 'February'),
            ('03', 'March'),
            ('04', 'April'),
            ('05', 'May'),
            ('06', 'June'),
            ('07', 'July'),
            ('08', 'August'),
            ('09', 'September'),
            ('10', 'October'),
            ('11', 'November'),
            ('12', 'December'),
            ], 'Period', sort=False, required=True,
        states={
            'readonly': Eval('state') != 'draft',
            }, depends=['state'])
    issued_lines = fields.One2Many('aeat.340.report.issued', 'report',
        'Issued', states={
            'readonly': Eval('state') != 'calculated',
            }, depends=['state'])
    received_lines = fields.One2Many('aeat.340.report.received', 'report',
        'Received', states={
            'readonly': Eval('state') != 'calculated',
            }, depends=['state'])
    investment_lines = fields.One2Many('aeat.340.report.investment', 'report',
        'Investement Operations', states={
            'readonly': Eval('state') != 'calculated',
            }, depends=['state'])
    intracommunity_lines = fields.One2Many('aeat.340.report.intracommunity',
        'report', 'Intracommunity Operations', states={
            'readonly': Eval('state') != 'calculated',
            }, depends=['state'])
    taxable_total = fields.Function(fields.Numeric('Taxable Total',
            digits=(16, 2),), 'get_totals')
    sharetax_total = fields.Function(fields.Numeric('Share Tax Total',
            digits=(16, 2),), 'get_totals')
    record_count = fields.Function(fields.Integer('Record Count'),
        'get_totals')
    total = fields.Function(fields.Numeric('Total', digits=(16, 2)),
        'get_totals')
    file_ = fields.Binary('File', filename='filename', states={
            'invisible': Eval('state') != 'done',
            })
    filename = fields.Function(fields.Char("File Name"),
        'get_filename')
    state = fields.Selection([
            ('draft', 'Draft'),
            ('calculated', 'Calculated'),
            ('done', 'Done'),
            ('cancelled', 'Cancelled')
            ], 'State', readonly=True)

    @classmethod
    def __setup__(cls):
        super(cls, Report).__setup__()
        cls._error_messages.update({
                'invalid_currency': ('Currency in AEAT 340 report "%s" must be'
                    ' Euro.')
                })
        cls._buttons.update({
                'draft': {
                    'invisible': ~Eval('state').in_(['calculated',
                            'cancelled']),
                    'icon': 'tryton-go-previous',
                    },
                'calculate': {
                    'invisible': ~Eval('state').in_(['draft']),
                    'icon': 'tryton-go-next',
                    },
                'process': {
                    'invisible': ~Eval('state').in_(['calculated']),
                    'icon': 'tryton-ok',
                    },
                'cancel': {
                    'invisible': Eval('state').in_(['cancelled']),
                    'icon': 'tryton-cancel',
                    },
                })
        cls._transitions |= set((
                ('draft', 'calculated'),
                ('draft', 'cancelled'),
                ('calculated', 'draft'),
                ('calculated', 'done'),
                ('calculated', 'cancelled'),
                ('done', 'cancelled'),
                ('cancelled', 'draft'),
                ))

    def get_rec_name(self, name):
        return '%s - %s/%s' % (self.company.rec_name,
            self.fiscalyear.name, self.period)

    @staticmethod
    def default_company():
        return Transaction().context.get('company')

    def get_currency(self, name):
        return self.company.currency.id

    @staticmethod
    def default_fiscalyear():
        FiscalYear = Pool().get('account.fiscalyear')
        return FiscalYear.find(
            Transaction().context.get('company'), exception=False)

    @fields.depends('fiscalyear')
    def on_change_with_fiscalyear_code(self):
        code = None
        if self.fiscalyear:
            code = self.fiscalyear.start_date.year
        return code

    @fields.depends('company')
    def on_change_with_company_vat(self):
        if self.company:
            return self.company.party.vat_number

    @staticmethod
    def default_type():
        return 'N'

    @staticmethod
    def default_support_type():
        return 'T'

    @staticmethod
    def default_state():
        return 'draft'

    @classmethod
    def get_totals(cls, reports, names):
        res = {
            'taxable_total': dict([(x.id, _ZERO) for x in reports]),
            'sharetax_total': dict([(x.id, _ZERO) for x in reports]),
            'record_count': dict([(x.id, 0) for x in reports]),
            'total': dict([(x.id, _ZERO) for x in reports]),
            }
        for report in reports:
            base = sum([x.base for x in report.lines])
            tax = sum([x.tax for x in report.lines])
            res['record_count'][report.id] += (len(report.issued_lines) +
                len(report.received_lines) + len(report.investment_lines) +
                len(report.intracommunity_lines))
            res['taxable_total'][report.id] = base
            res['sharetax_total'][report.id] = tax
            res['total'][report.id] += base + tax
        for x in res.keys():
            if x not in names:
                del res[x]
        return res

    def get_filename(self, name):
        return 'aeat340-%s-%s.txt' % (
            self.fiscalyear_code, self.period)

    @property
    def lines(self):
        return itertools.chain(self.issued_lines, self.received_lines,
            self.investment_lines, self.intracommunity_lines)

    @classmethod
    def validate(cls, reports):
        for report in reports:
            report.check_euro()

    def check_euro(self):
        if self.currency.code != 'EUR':
            self.raise_user_error('invalid_currency', self.rec_name)

    @classmethod
    @ModelView.button
    @Workflow.transition('draft')
    def draft(cls, reports):
        cls._delete_lines(reports)

    @classmethod
    @ModelView.button
    @Workflow.transition('calculated')
    def calculate(cls, reports):
        pool = Pool()
        Data = pool.get('aeat.340.record')
        Issued = pool.get('aeat.340.report.issued')
        Received = pool.get('aeat.340.report.received')
        Investment = pool.get('aeat.340.report.investment')
        Intracomunity = pool.get('aeat.340.report.intracommunity')

        cls._delete_lines(reports)

        issued_to_create = {}
        received_to_create = {}
        investment_to_create = {}
        intracomunity_to_create = {}
        for report in reports:
            fiscalyear = report.fiscalyear
            multiplier = 1
            period = report.period
            if 'T' in period:
                period = int(period[0]) - 1
                multiplier = 3
                start_month = period * multiplier + 1
            else:
                start_month = int(period) * multiplier

            end_month = start_month + multiplier

            to_create = {}
            for record in Data.search([
                    ('fiscalyear', '=', fiscalyear.id),
                    ('month', '>=', start_month),
                    ('month', '<', end_month)
                    ]):
                key = '%s-%s-%s-%s-%s' % (report.id, record.invoice.id,
                    record.book_key, record.operation_key, record.tax_rate)

                issued = received = False
                if record.book_key in ['E', 'F']:
                    to_create = issued_to_create
                    issued = True
                elif record.book_key in ['R', 'S']:
                    to_create = received_to_create
                    received = True
                elif record.book_key in ['I', 'J']:
                    to_create = investment_to_create
                else:
                    to_create = intracomunity_to_create

                if 'credit_note' in record.invoice.type:
                    sign = -1
                else:
                    sign = 1
                if record.operation_key == 'D':
                    assert 'credit_note' in record.invoice.type

                if key in to_create:
                    to_create[key]['base'] += record.base * sign
                    to_create[key]['tax'] += record.tax * sign
                    if record.equivalence_tax and issued:
                        to_create[key]['equivalence_tax'] += (
                            record.equivalence_tax * sign)
                    to_create[key]['total'] += record.total * sign
                    if (record.operation_key == 'B' and record.ticket_count
                            and (issued or received)):
                        if issued:
                            to_create[key]['issued_invoice_count'] += (
                                record.ticket_count)
                        elif received:
                            to_create[key]['received_invoice_count'] += (
                                record.ticket_count)
                        first_inv_number, last_inv_number = (
                            record.get_first_last_invoice_number())
                        if (first_inv_number
                                and to_create[key]['first_invoice_number']
                                and first_inv_number
                                < to_create[key]['first_invoice_number']):
                            to_create[key]['first_invoice_number'] = (
                                first_inv_number)
                        if (last_inv_number
                                and to_create[key]['last_invoice_number']
                                and last_inv_number
                                < to_create[key]['last_invoice_number']):
                            to_create[key]['last_invoice_number'] = (
                                last_inv_number)
                    to_create[key]['records'][0][1].append(record.id)
                else:
                    to_create[key] = {
                        # TODO: set company?
                        'report': report.id,
                        'party_nif': record.party_nif,
                        # TODO: set representative_nif?
                        'party_name': record.party_name[:40],
                        'party_country': record.party_country,
                        'party_identifier_type': record.party_identifier_type,
                        # TODO: set party_identifier?
                        'book_key': record.book_key,
                        'operation_key': record.operation_key,
                        'issue_date': record.issue_date,
                        'operation_date': record.operation_date,
                        'tax_rate': record.tax_rate,
                        'base': record.base * sign,
                        'tax': record.tax * sign,
                        'total': record.total * sign,
                        # TODO: set cost?
                        'invoice_number': record.invoice_number[:40],
                        'record_number': (record.invoice.move.number
                            if record.invoice and record.invoice.move
                            else None),
                        'records': [('add', [record.id])],
                        }
                    if issued or received:
                        to_create[key]['record_count'] = (
                            len(record.invoice.aeat340_records)
                            if record.operation_key == 'C' else 1),
                        if issued:
                            to_create[key].update({
                                    'equivalence_tax': record.equivalence_tax,
                                    'equivalence_tax_rate': (
                                        record.equivalence_tax_rate),
                                    'issued_invoice_count': 1,
                                    })
                        elif received:
                            to_create[key]['received_invoice_count'] = 1
                    if record.operation_key == 'B' and (issued or received):
                        if issued:
                            to_create[key]['issued_invoice_count'] = (
                                record.ticket_count or 1)
                        elif received:
                            to_create[key]['received_invoice_count'] = (
                                record.ticket_count or 1)
                        first_inv_number, last_inv_number = (
                            record.get_first_last_invoice_number())
                        to_create[key]['first_invoice_number'] = (
                            first_inv_number or '1')
                        to_create[key]['last_invoice_number'] = (
                            last_inv_number or '1')
                    elif record.operation_key == 'C':
                        # TODO: set number of records related to same invoice
                        pass
                    elif (record.operation_key == 'D'
                            and record.corrective_invoice_number
                            and issued):
                        to_create[key]['corrective_invoice_number'] = (
                            record.corrective_invoice_number[:40])

        with Transaction().set_context(_check_access=False):
            Issued.create(sorted(issued_to_create.values(),
                    key=lambda x: x['issue_date']))
            Received.create(sorted(received_to_create.values(),
                    key=lambda x: x['issue_date']))
            Investment.create(sorted(investment_to_create.values(),
                    key=lambda x: x['issue_date']))
            Intracomunity.create(sorted(intracomunity_to_create.values(),
                    key=lambda x: x['issue_date']))

        cls.write(reports, {
                'calculation_date': datetime.datetime.now(),
                })

    @classmethod
    def _delete_lines(cls, reports):
        pool = Pool()
        Issued = pool.get('aeat.340.report.issued')
        Received = pool.get('aeat.340.report.received')
        Investment = pool.get('aeat.340.report.investment')
        Intracomunity = pool.get('aeat.340.report.intracommunity')
        report_ids = [r.id for r in reports]
        with Transaction().set_context(from_report=True, _check_access=False):
            Issued.delete(Issued.search([('report', 'in', report_ids)]))
            Received.delete(Received.search([('report', 'in', report_ids)]))
            Investment.delete(Investment.search(
                [('report', 'in', report_ids)]))
            Intracomunity.delete(Intracomunity.search(
                [('report', 'in', report_ids)]))

    @classmethod
    @ModelView.button
    @Workflow.transition('done')
    def process(cls, reports):
        for report in reports:
            report.create_file()

    @classmethod
    @ModelView.button
    @Workflow.transition('cancelled')
    def cancel(cls, reports):
        pass

    def create_file(self):
        records = []
        record = retrofix.Record(aeat340.PRESENTER_HEADER_RECORD)
        record.fiscalyear = str(self.fiscalyear_code)
        record.nif = self.company_vat
        record.presenter_name = self.company.party.name.upper()
        record.support_type = self.support_type
        record.contact_phone = self.contact_phone
        record.contact_name = self.contact_name.upper()
        # record.declaration_number = int('340{}{}{:0>4}'.format(
        #     self.fiscalyear_code,
        #     self.period,
        #     <autoincrement>))
        record.declaration_number = '0'
        # record.complementary =
        # record.replacement =
        record.previous_declaration_number = self.previous_number or '0'
        record.period = self.period
        record.record_count = self.record_count
        record.total_base = self.taxable_total
        record.total_tax = self.sharetax_total
        record.total = self.total
        # record.representative_nif =
        records.append(record)
        for line in self.lines:
            record = line.get_record()
            record.fiscalyear = str(self.fiscalyear_code)
            record.nif = self.company_vat
            records.append(record)

        data = retrofix.record.write(records)
        data = remove_accents(data).upper()
        if isinstance(data, unicode):
            data = data.encode('iso-8859-1')
        self.file_ = bytes(data)
        self.save()


class LineMixin(object):
    _rec_name = 'party_name'

    company = fields.Many2One('company.company', 'Company', required=True)
    report = fields.Many2One('aeat.340.report', 'Report', ondelete='CASCADE')
    party_nif = fields.Char('Party CIF/NIF', size=9)
    representative_nif = fields.Char('Representative NIF', size=9)
    party_name = fields.Char('Party Name', size=40)
    party_country = fields.Char('Party Country', size=2)
    party_identifier_type = fields.Selection(PARTY_IDENTIFIER_TYPE,
        'Party Identifier Type', required=True)
    party_identifier = fields.Char('Party Identifier', size=20)
    book_key = fields.Selection(BOOK_KEY, 'Book Key',
        sort=False, required=True)
    operation_key = fields.Selection(OPERATION_KEY, 'Operation Key',
        sort=False, required=True)
    issue_date = fields.Date('Issue Date', required=True)
    operation_date = fields.Date('Operation Date', required=True)
    tax_rate = fields.Numeric('Tax Rate', digits=(16, 2), required=True)
    base = fields.Numeric('Base', digits=(16, 2), required=True)
    tax = fields.Numeric('Tax', digits=(16, 2), required=True)
    total = fields.Numeric('Total', digits=(16, 2), required=True)
    cost = fields.Numeric('Cost', digits=(16, 2),
        states={
            'required': Eval('operation_key') == 'G',
            'invisible': Eval('operation_key') != 'G',
            })
    invoice_number = fields.Char('Invoice Number', size=40)
    record_number = fields.Char('Record Number', size=18)

    @classmethod
    def __setup__(cls):
        super(LineMixin, cls).__setup__()
        cls._error_messages.update({
                'check_state_invalid': ('Line "%s" cannot be modified because '
                    'its report is not in Calculated state.'),
                'delete_state_invalid': ('Line "%s" cannot be deleted because '
                    'its report is not in Draft state.'),
                'invalid_book_key': ('Invalid Book Key "%(key)s" for record '
                    '"%(record)s".')
                })

    def get_rec_name(self, name):
        report = self.report.rec_name + ':' if self.report else ''
        return "%s %s-%s : %s" % (report, self.book_key,
                self.operation_key, self.invoice_number)

    @staticmethod
    def default_company():
        return Transaction().context.get('company')

    @fields.depends('operation_key', 'cost')
    def on_change_with_cost(self):
        if self.operation_key == 'G':
            return None
        return self.cost

    def set_values(self, record):
        columns = [x for x in dir(self.__class__)
            if isinstance(getattr(self.__class__, x), fields.Field)]
        columns = [x for x in columns if x in record._fields]
        for column in columns:
            value = getattr(self, column)
            if value:
                setattr(record, column, getattr(self, column))

    @classmethod
    def validate(cls, lines):
        super(LineMixin, cls).validate(lines)
        for line in lines:
            line.check_key()

    def check_key(self):
        if self._possible_keys and self.book_key not in self._possible_keys:
            self.raise_user_error('invalid_book_key', {
                    'key': self.book_key,
                    'record': self.rec_name,
                    })

    @classmethod
    def write(cls, *args):
        actions = iter(args)
        for lines, _ in zip(actions, actions):
            for line in lines:
                line.check_state()
        super(LineMixin, cls).write(*args)

    def check_state(self):
        if self.report and self.report.state != 'calculated':
            self.raise_user_error('check_state_invalid', self.rec_name)

    @classmethod
    def delete(cls, lines):
        if not Transaction().context.get('from_report'):
            for line in lines:
                if (line.report
                        and line.report.state not in ('draft', 'calculated')):
                    cls.raise_user_error('delete_state_invalid', line.rec_name)
        super(LineMixin, cls).delete(lines)


class Issued(LineMixin, ModelSQL, ModelView):
    '''
    AEAT 340 Issued
    '''
    __name__ = 'aeat.340.report.issued'
    issued_invoice_count = fields.Integer('Issued Invoice Count')
    record_count = fields.Integer('Record Count')
    first_invoice_number = fields.Char('First Invoice Number', size=40)
    last_invoice_number = fields.Char('Last Invoice Number', size=40)
    corrective_invoice_number = fields.Char('Corrective Invoice Number',
        size=40)
    equivalence_tax_rate = fields.Numeric('Equivalence Tax Rate',
        digits=(16, 2))
    equivalence_tax = fields.Numeric('Equivalence Tax', digits=(16, 2))
    property_state = fields.Selection(PROPERTY_STATE, 'Property State',
        required=True, sort=False)
    cadaster_number = fields.Char('Cadaster Number', size=25)
    cash_amount = fields.Numeric('Cash Amount', digits=(16, 2))
    invoice_fiscalyear = fields.Integer('Fiscal Year')
    property_transfer_amount = fields.Numeric('Property Transfer Amount',
        digits=(16, 2))
    records = fields.One2Many('aeat.340.record', 'issued',
        'AEAT 340 Records', readonly=True)

    _possible_keys = ['E', 'F']

    @classmethod
    def __register__(cls, module_name):
        TableHandler = backend.get('TableHandler')
        cursor = Transaction().cursor
        sql_table = cls.__table__()

        # Migration from 3.4.0: renamed invoice_count to issued_invoice_count
        table = TableHandler(cursor, cls, module_name)
        copy_issued_inv_count = (table.column_exist('invoice_count')
            and not table.column_exist('issued_invoice_count'))

        # Migration from 3.4.4: changed type and required of property_state
        if table.column_exist('property_state'):
            cursor.execute(
                *sql_table.select(where=sql_table.property_state != Null))
            if not cursor.fetchone():
                table.drop_column('property_state')

        super(Issued, cls).__register__(module_name)

        # Migration from 3.4.0: renamed invoice_count to issued_invoice_count
        if copy_issued_inv_count:
            cursor.execute(*sql_table.update(
                columns=[sql_table.issued_invoice_count],
                values=[sql_table.invoice_count]))
            table.drop_column('invoice_count')

    @staticmethod
    def default_property_state():
        return '0'

    @fields.depends('operation_key', 'property_state')
    def on_change_with_property_state(self):
        if self.operation_key != 'R':
            return self.default_property_state()
        return self.property_state

    @fields.depends('operation_key', 'cadaster_number')
    def on_change_with_cadaster_number(self):
        if self.operation_key == 'R':
            return None
        return self.cadaster_number

    def get_record(self):
        record = retrofix.Record(aeat340.ISSUED_RECORD)
        self.set_values(record)
        return record


class Received(LineMixin, ModelSQL, ModelView):
    '''
    AEAT 340 Received
    '''
    __name__ = 'aeat.340.report.received'
    received_invoice_count = fields.Integer('Received Invoice Count')
    record_count = fields.Integer('Record Count')
    first_invoice_number = fields.Char('First Invoice Number', size=40)
    last_invoice_number = fields.Char('Last Invoice Number', size=40)
    deducible_amount = fields.Numeric('Deducible Amount', digits=(16, 2))
    records = fields.One2Many('aeat.340.record', 'received',
        'AEAT 340 Records', readonly=True)

    _possible_keys = ['R', 'S']

    @classmethod
    def __register__(cls, module_name):
        TableHandler = backend.get('TableHandler')
        cursor = Transaction().cursor
        sql_table = cls.__table__()

        # Migration from 3.4.1: renamed invoice_count to received_invoice_count
        table = TableHandler(cursor, cls, module_name)
        copy_issued_inv_count = (table.column_exist('invoice_count')
            and not table.column_exist('received_invoice_count'))

        super(Received, cls).__register__(module_name)

        # Migration from 3.4.1: renamed invoice_count to received_invoice_count
        if copy_issued_inv_count:
            cursor.execute(*sql_table.update(
                columns=[sql_table.received_invoice_count],
                values=[sql_table.invoice_count]))
            table.drop_column('invoice_count')

    def get_record(self):
        record = retrofix.Record(aeat340.RECEIVED_RECORD)
        self.set_values(record)
        return record


class Investment(LineMixin, ModelSQL, ModelView):
    '''
    AEAT 340 Investment
    '''
    __name__ = 'aeat.340.report.investment'
    pro_rata = fields.Integer('Pro Rata')
    yearly_regularitzation = fields.Numeric('Yearly Regularization',
        digits=(16, 2))
    submission_number = fields.Char('Submission Number', size=40)
    transmissions = fields.Numeric('Transmissions', digits=(16, 2))
    usage_start_date = fields.Date('Usage Start Date')
    good_identifier = fields.Char('Good Identifier', size=17)
    records = fields.One2Many('aeat.340.record', 'investment',
        'AEAT 340 Records', readonly=True)

    _possible_keys = ['I', 'J']

    def get_record(self):
        record = retrofix.Record(aeat340.INVESTMENT_RECORD)
        self.set_values(record)
        return record


class Intracommunity(LineMixin, ModelSQL, ModelView):
    '''
    AEAT 340 Intracommunity
    '''
    __name__ = 'aeat.340.report.intracommunity'
    intracommunity_operation_type = fields.Integer(
        'Intracommunity Operation Type')
    declaring_key = fields.Char('Declared Key', size=1)
    intracommunity_country = fields.Char('Intracommunity Country', size=2)
    operation_term = fields.Integer('Operation Term')
    goods_description = fields.Char('Goods Description', size=35)
    party_street = fields.Char('Party Street', size=40)
    party_city = fields.Char('Party City', size=22)
    party_zip = fields.Char('Party Zip', size=10)
    other_documentation = fields.Char('Other Documentation', size=135)
    records = fields.One2Many('aeat.340.record', 'intracommunity',
        'AEAT 340 Records', readonly=True)

    _possible_keys = ['U']

    def get_record(self):
        record = retrofix.Record(aeat340.INTRACOMMUNITY_RECORD)
        self.set_values(record)
        return record
