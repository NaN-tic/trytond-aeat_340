# The COPYRIGHT file at the top level of this repository contains the full
# copyright notices and license terms.
import itertools
import datetime
import retrofix
from retrofix import aeat340
from decimal import Decimal

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
    ('A', 'A - Invoice\'s Summary'),
    ('B', 'B - Ticket\'s Summary'),
    ('C', 'C - Invoice with several taxes'),
    ('D', 'D - Credit Note'),
#    ('E', '(optional)'),
    ('F', 'F - Travel agencies acquisitions'),
    ('G', 'G - Special arrangment parties on IVA or IGIC'),
    ('H', 'H - Gold inversion special arrangment'),
    ('I', 'I - Passive subject inversion'),
    ('J', 'J - Tickets'),
    ('K', 'K - Rectification of registry mistakes'),
    ('L', 'L - Acquisitions to retailers of IGIC'),
#    ('M', '(optional)'),
    ('N', 'N - Services to travel agencies'),
#    ('O', '(optional)'),
#    ('P', '(optional)'),
#    ('Q', '(optional)'),
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


class Report(Workflow, ModelSQL, ModelView):
    '''
    AEAT 340 Report
    '''
    __name__ = 'aeat.340.report'
    company = fields.Many2One('company.company', 'Company', required=True,
        states={
            'readonly': Eval('state') == 'done',
            }, depends=['state'])
    currency = fields.Function(fields.Many2One('currency.currency',
        'Currency'), 'get_currency')
    previous_number = fields.Char('Previous Declaration Number', size=13,
        states={
            'readonly': Eval('state') == 'done',
            }, depends=['state'])
    representative_vat = fields.Char('L.R. VAT number', size=9,
        help='Legal Representative VAT number.', states={
            'required': Eval('state') == 'calculated',
            'readonly': Eval('state') == 'done',
            }, depends=['state'])
    fiscalyear = fields.Many2One('account.fiscalyear', 'Fiscal Year',
        states={
            'readonly': Eval('state') == 'done',
            }, depends=['state'])
    fiscalyear_code = fields.Integer('Fiscal Year Code', required=True)
    company_vat = fields.Char('VAT', size=9, states={
            'required': Eval('state') == 'calculated',
            'readonly': Eval('state') == 'done',
            }, depends=['state'])
    type = fields.Selection([
            ('N', 'Normal'),
            ('C', 'Complementary'),
            ('S', 'Substitutive')
            ], 'Statement Type', required=True, states={
                'readonly': Eval('state') == 'done',
            }, depends=['state'])
    support_type = fields.Selection([
            ('C', 'DVD'),
            ('T', 'Telematics'),
            ], 'Support Type', required=True, states={
                'readonly': Eval('state') == 'done',
            }, depends=['state'])
    calculation_date = fields.DateTime('Calculation Date', readonly=True)
    state = fields.Selection([
            ('draft', 'Draft'),
            ('calculated', 'Calculated'),
            ('done', 'Done'),
            ('cancelled', 'Cancelled')
            ], 'State', readonly=True)
    contact_phone = fields.Char('Phone', size=9)
    contact_name = fields.Char('Name And Surname Contact', size=40)
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
            ], 'Period', sort=False, required=True)
    issued_lines = fields.One2Many('aeat.340.report.issued', 'report',
        'Issued')
    received_lines = fields.One2Many('aeat.340.report.received', 'report',
        'Received')
    investment_lines = fields.One2Many('aeat.340.report.investment', 'report',
        'Investement Operations')
    intracommunity_lines = fields.One2Many('aeat.340.report.intracommunity',
        'report', 'Intracommunity Operations')
    taxable_total = fields.Function(fields.Numeric('Taxable Total',
            digits=(16, 2),), 'get_totals')
    sharetax_total = fields.Function(fields.Numeric('Share Tax Total',
            digits=(16, 2),), 'get_totals')
    record_count = fields.Function(fields.Integer('Record Count'),
        'get_totals')
    total = fields.Function(fields.Numeric('Total', digits=(16, 2)),
        'get_totals')
    file_ = fields.Binary('File', states={
            'invisible': Eval('state') != 'done',
            })

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

    @staticmethod
    def default_support_type():
        return 'T'

    @staticmethod
    def default_type():
        return 'N'

    @staticmethod
    def default_state():
        return 'draft'

    @staticmethod
    def default_company():
        return Transaction().context.get('company')

    @staticmethod
    def default_fiscalyear():
        FiscalYear = Pool().get('account.fiscalyear')
        return FiscalYear.find(
            Transaction().context.get('company'), exception=False)

    def get_rec_name(self, name):
        return '%s - %s/%s' % (self.company.rec_name,
            self.fiscalyear.name, self.period)

    def get_currency(self, name):
        return self.company.currency.id

    @fields.depends('fiscalyear')
    def on_change_with_fiscalyear_code(self):
        code = self.fiscalyear.code if self.fiscalyear else None
        if code:
            try:
                code = int(code)
            except ValueError:
                code = None
        else:
            code = None
        return code

    @classmethod
    def validate(cls, reports):
        for report in reports:
            report.check_euro()

    def check_euro(self):
        if self.currency.code != 'EUR':
            self.raise_user_error('invalid_currency', self.rec_name)

    @property
    def lines(self):
        return itertools.chain(self.issued_lines, self.received_lines,
            self.investment_lines, self.intracommunity_lines)

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
        transaction = Transaction()
        with transaction.set_user(0) and \
                transaction.set_context(from_report=True):
            Issued.delete(Issued.search([
                ('report', 'in', [r.id for r in reports])]))
            Received.delete(Received.search([
                ('report', 'in', [r.id for r in reports])]))
            Investment.delete(Investment.search([
                ('report', 'in', [r.id for r in reports])]))
            Intracomunity.delete(Intracomunity.search([
                ('report', 'in', [r.id for r in reports])]))

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

                if record.book_key in ['E', 'F']:
                    to_create = issued_to_create
                elif record.book_key in ['R', 'S']:
                    to_create = received_to_create
                elif record.book_key in ['I', 'J']:
                    to_create = investment_to_create
                else:
                    to_create = intracomunity_to_create
                if key in to_create:
                    to_create[key]['base'] += record.base
                    to_create[key]['tax'] += record.tax
                    to_create[key]['total'] += record.total
                    to_create[key]['records'][0][1].append(record.id)
                else:
                    to_create[key] = {
                        'base': record.base,
                        'party_name': record.party_name[:40],
                        'party_nif': record.party_nif,
                        'party_country': record.party_country,
                        'party_identifier_type': record.party_identifier_type,
                        'base': record.base,
                        'tax': record.tax,
                        'tax_rate': record.tax_rate,
                        'total': record.total,
                        'operation_key': record.operation_key,
                        'book_key': record.book_key,
                        'issue_date': record.issue_date,
                        'operation_date': record.operation_date,
                        'report': report.id,
                        'records': [('add', [record.id])],
                    }
        with transaction.set_user(0):
            Issued.create(issued_to_create.values())
            Received.create(received_to_create.values())
            Investment.create(investment_to_create.values())
            Intracomunity.create(intracomunity_to_create.values())

        cls.write(reports, {
                'calculation_date': datetime.datetime.now(),
                })

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

    @classmethod
    @ModelView.button
    @Workflow.transition('draft')
    def draft(cls, reports):
        pass

    def create_file(self):
        records = []
        record = retrofix.Record(aeat340.PRESENTER_HEADER_RECORD)
        record.fiscalyear = str(self.fiscalyear_code)
        record.nif = self.company_vat
        # record.presenter_name =
        record.support_type = self.support_type
        record.contact_phone = self.contact_phone
        record.contact_name = self.contact_name
        # record.declaration_number =
        # record.complementary =
        # record.replacement =
        record.previous_declaration_number = self.previous_number
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
        if isinstance(data, unicode):
            data = data.encode('iso-8859-1')
        self.file_ = buffer(data)
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
                    'its report is not in draft state.'),
                'delete_state_invalid': ('Line "%s" cannot be deleted because '
                    'its report is not in draft state.'),
                'invalid_book_key': ('Invalid Book Key "%(key)s" for record '
                    '"%(record)s".')
                })

    @staticmethod
    def default_company():
        return Transaction().context.get('company')

    @classmethod
    def validate(cls, lines):
        super(LineMixin, cls).validate(lines)
        for line in lines:
            line.check_state()
            line.check_key()

    def check_state(self):
        if self.report and self.report.state != 'draft':
            self.raise_user_error('check_state_invalid', self.rec_name)

    def get_rec_name(self, name):
        report = self.report.rec_name + ':' if self.report else ''
        return "%s %s-%s : %s" % (report, self.book_key,
                self.operation_key, self.invoice_number)

    def check_key(self):
        if self._possible_keys and self.book_key not in self._possible_keys:
            self.raise_user_error('invalid_book_key',
                {'key': self.book_key, 'record': self.rec_name})

    @classmethod
    def delete(cls, lines):
        if not Transaction().context.get('from_report'):
            for line in lines:
                if line.report and line.report.state != 'draft':
                    cls.raise_user_error('delete_state_invalid', line.rec_name)
        super(LineMixin, cls).delete(lines)

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


class Issued(LineMixin, ModelSQL, ModelView):
    '''
    AEAT 340 Issued
    '''
    __name__ = 'aeat.340.report.issued'
    invoice_count = fields.Integer('Invoice Count')
    record_count = fields.Integer('Record Count')
    first_invoice_number = fields.Char('First Invoice Number', size=40)
    last_invoice_number = fields.Char('Last Invoice Number', size=40)
    corrective_invoice_number = fields.Char('Corrective Invoice Number',
        size=40)
    equivalence_tax_rate = fields.Numeric('Equivalence Tax Rate',
        digits=(16, 2))
    equivalence_tax = fields.Numeric('Equivalence Tax', digits=(16, 2))
    property_state = fields.Char('Property State', size=1)
    cadaster_number = fields.Char('Cadaster Number', size=25)
    cash_amount = fields.Numeric('Cash Amount', digits=(16, 2))
    invoice_fiscalyear = fields.Integer('Fiscal Year')
    property_transfer_amount = fields.Numeric('Property Transfer Amount',
        digits=(16, 2))
    records = fields.One2Many('aeat.340.record', 'issued',
        'AEAT 340 Records', readonly=True)

    _possible_keys = ['E', 'F']

    @fields.depends('operation_key', 'property_state')
    def on_change_with_property_state(self):
        if self.operation_key == 'R':
            return None
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
    invoice_count = fields.Integer('Invoice Count')
    record_count = fields.Integer('Record Count')
    first_invoice_number = fields.Char('First Invoice Number', size=40)
    last_invoice_number = fields.Char('Last Invoice Number', size=40)
    deducible_amount = fields.Numeric('Deducible Amount', digits=(16, 2))
    records = fields.One2Many('aeat.340.record', 'received',
        'AEAT 340 Records', readonly=True)

    _possible_keys = ['R', 'S']

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
