================
Invoice Scenario
================

Imports::
    >>> import datetime
    >>> from dateutil.relativedelta import relativedelta
    >>> from decimal import Decimal
    >>> from operator import attrgetter
    >>> from proteus import config, Model, Wizard
    >>> today = datetime.date.today()

Create database::

    >>> config = config.set_trytond()
    >>> config.pool.test = True

Install account_invoice::

    >>> Module = Model.get('ir.module.module')
    >>> aeat_340_module, = Module.find(
    ...     [('name', '=', 'aeat_340')])
    >>> Module.install([aeat_340_module.id], config.context)
    >>> Wizard('ir.module.module.install_upgrade').execute('upgrade')

Create company::

    >>> Currency = Model.get('currency.currency')
    >>> CurrencyRate = Model.get('currency.currency.rate')
    >>> currencies = Currency.find([('code', '=', 'EUR')])
    >>> if not currencies:
    ...     currency = Currency(name='Euro', symbol=u'€', code='EUR',
    ...         rounding=Decimal('0.01'), mon_grouping='[]',
    ...         mon_decimal_point='.')
    ...     currency.save()
    ...     CurrencyRate(date=today + relativedelta(month=1, day=1),
    ...         rate=Decimal('1.0'), currency=currency).save()
    ... else:
    ...     currency, = currencies
    >>> Company = Model.get('company.company')
    >>> Party = Model.get('party.party')
    >>> company_config = Wizard('company.company.config')
    >>> company_config.execute('company')
    >>> company = company_config.form
    >>> party = Party(name='Dunder Mifflin')
    >>> party.save()
    >>> company.party = party
    >>> company.currency = currency
    >>> company_config.execute('add')
    >>> company, = Company.find([])

Reload the context::

    >>> User = Model.get('res.user')
    >>> config._context = User.get_preferences(True, config.context)

Create fiscal year::

    >>> FiscalYear = Model.get('account.fiscalyear')
    >>> Sequence = Model.get('ir.sequence')
    >>> SequenceStrict = Model.get('ir.sequence.strict')
    >>> fiscalyear = FiscalYear(name=str(today.year))
    >>> fiscalyear.start_date = today + relativedelta(month=1, day=1)
    >>> fiscalyear.end_date = today + relativedelta(month=12, day=31)
    >>> fiscalyear.company = company
    >>> post_move_seq = Sequence(name=str(today.year), code='account.move',
    ...     company=company)
    >>> post_move_seq.save()
    >>> fiscalyear.post_move_sequence = post_move_seq
    >>> invoice_seq = SequenceStrict(name=str(today.year),
    ...     code='account.invoice', company=company)
    >>> invoice_seq.save()
    >>> fiscalyear.out_invoice_sequence = invoice_seq
    >>> fiscalyear.in_invoice_sequence = invoice_seq
    >>> fiscalyear.out_credit_note_sequence = invoice_seq
    >>> fiscalyear.in_credit_note_sequence = invoice_seq
    >>> fiscalyear.save()
    >>> FiscalYear.create_period([fiscalyear.id], config.context)

Create chart of accounts::

    >>> AccountTemplate = Model.get('account.account.template')
    >>> Account = Model.get('account.account')
    >>> account_template, = AccountTemplate.find([('parent', '=', None)])
    >>> create_chart = Wizard('account.create_chart')
    >>> create_chart.execute('account')
    >>> create_chart.form.account_template = account_template
    >>> create_chart.form.company = company
    >>> create_chart.execute('create_account')
    >>> receivable, = Account.find([
    ...         ('kind', '=', 'receivable'),
    ...         ('company', '=', company.id),
    ...         ])
    >>> payable, = Account.find([
    ...         ('kind', '=', 'payable'),
    ...         ('company', '=', company.id),
    ...         ])
    >>> revenue, = Account.find([
    ...         ('kind', '=', 'revenue'),
    ...         ('company', '=', company.id),
    ...         ])
    >>> expense, = Account.find([
    ...         ('kind', '=', 'expense'),
    ...         ('company', '=', company.id),
    ...         ])
    >>> account_tax, = Account.find([
    ...         ('kind', '=', 'other'),
    ...         ('company', '=', company.id),
    ...         ('name', '=', 'Main Tax'),
    ...         ])
    >>> create_chart.form.account_receivable = receivable
    >>> create_chart.form.account_payable = payable
    >>> create_chart.execute('create_properties')

Create tax::

    >>> TaxCode = Model.get('account.tax.code')
    >>> Tax = Model.get('account.tax')
    >>> tax = Tax()
    >>> tax.name = 'Tax'
    >>> tax.description = 'Tax'
    >>> tax.type = 'percentage'
    >>> tax.rate = Decimal('.10')
    >>> tax.invoice_account = account_tax
    >>> tax.credit_note_account = account_tax
    >>> invoice_base_code = TaxCode(name='invoice base')
    >>> invoice_base_code.save()
    >>> tax.invoice_base_code = invoice_base_code
    >>> invoice_tax_code = TaxCode(name='invoice tax')
    >>> invoice_tax_code.save()
    >>> tax.invoice_tax_code = invoice_tax_code
    >>> credit_note_base_code = TaxCode(name='credit note base')
    >>> credit_note_base_code.save()
    >>> tax.credit_note_base_code = credit_note_base_code
    >>> credit_note_tax_code = TaxCode(name='credit note tax')
    >>> credit_note_tax_code.save()
    >>> tax.credit_note_tax_code = credit_note_tax_code
    >>> Type = Model.get('aeat.340.type')
    >>> etype = Type()
    >>> etype.book_key = 'E'
    >>> etype.save()
    >>> itype = Type()
    >>> itype.book_key = 'I'
    >>> itype.save()
    >>> rtype = Type()
    >>> rtype.book_key = 'R'
    >>> rtype.save()
    >>> utype = Type()
    >>> utype.book_key = 'U'
    >>> utype.save()
    >>> tax.aeat340_book_keys.extend([etype, rtype, utype, itype])
    >>> tax.aeat340_default_in_book_key = rtype
    >>> tax.aeat340_default_out_book_key = etype
    >>> tax.save()
    >>> tax.reload()
    >>> len(tax.aeat340_book_keys) == 4
    True
    >>> tax.aeat340_default_in_book_key == rtype
    True
    >>> tax.aeat340_default_out_book_key == etype
    True

Create party::

    >>> Party = Model.get('party.party')
    >>> party = Party(name='Party', vat_number='00000000T')
    >>> party.save()

Create product::

    >>> ProductUom = Model.get('product.uom')
    >>> unit, = ProductUom.find([('name', '=', 'Unit')])
    >>> ProductTemplate = Model.get('product.template')
    >>> Product = Model.get('product.product')
    >>> product = Product()
    >>> template = ProductTemplate()
    >>> template.name = 'product'
    >>> template.default_uom = unit
    >>> template.type = 'service'
    >>> template.list_price = Decimal('40')
    >>> template.cost_price = Decimal('25')
    >>> template.account_expense = expense
    >>> template.account_revenue = revenue
    >>> template.customer_taxes.append(tax)
    >>> tax = Tax(1)
    >>> template.supplier_taxes.append(tax)
    >>> template.save()
    >>> product.template = template
    >>> product.save()

Create payment term::

    >>> PaymentTerm = Model.get('account.invoice.payment_term')
    >>> PaymentTermLine = Model.get('account.invoice.payment_term.line')
    >>> payment_term = PaymentTerm(name='Term')
    >>> payment_term_line = PaymentTermLine(type='percent', days=20,
    ...     percentage=Decimal(50))
    >>> payment_term.lines.append(payment_term_line)
    >>> payment_term_line = PaymentTermLine(type='remainder', days=40)
    >>> payment_term.lines.append(payment_term_line)
    >>> payment_term.save()

Create out invoice::

    >>> Record = Model.get('aeat.340.record')
    >>> Invoice = Model.get('account.invoice')
    >>> InvoiceLine = Model.get('account.invoice.line')
    >>> invoice = Invoice()
    >>> invoice.party = party
    >>> invoice.payment_term = payment_term
    >>> line = InvoiceLine()
    >>> invoice.lines.append(line)
    >>> line.product = product
    >>> line.quantity = 5
    >>> len(line.taxes) == 1
    True
    >>> line.aeat340_book_key == etype
    True
    >>> line.aeat340_operation_key == 'C'
    True
    >>> line.amount == Decimal(200)
    True
    >>> line = InvoiceLine()
    >>> invoice.lines.append(line)
    >>> len(line.taxes) == 0
    True
    >>> line.account = revenue
    >>> line.description = 'Test 2'
    >>> line.quantity = 1
    >>> line.unit_price = Decimal(40)
    >>> tax = Tax(1)
    >>> line.taxes.append(tax)
    >>> line.aeat340_book_key = itype
    >>> line.aeat340_operation_key = 'A'
    >>> line.amount == Decimal(40)
    True
    >>> invoice.save()
    >>> Invoice.post([invoice.id], config.context)
    >>> (rec1, rec2) = Record.find([('invoice', '=', invoice.id)])
    >>> rec1.party_name == 'Party'
    True
    >>> rec1.party_nif == '00000000T'
    True
    >>> rec1.month == today.month
    True
    >>> rec1.book_key == 'I'
    True
    >>> rec1.operation_key == 'A'
    True
    >>> rec1.base == Decimal(240)
    True
    >>> rec1.tax_rate == Decimal(10)
    True
    >>> rec1.tax == Decimal(24)
    True
    >>> rec1.total == Decimal(264)
    True
    >>> rec2.book_key == 'E'
    True
    >>> rec2.operation_key == 'C'
    True
    >>> rec2.base == Decimal(240)
    True
    >>> rec2.tax_rate == Decimal(10)
    True
    >>> rec2.tax == Decimal(24)
    True
    >>> rec2.total == Decimal(264)
    True

Create in invoice::

    >>> invoice = Invoice()
    >>> invoice.type = 'in_invoice'
    >>> invoice.party = party
    >>> invoice.payment_term = payment_term
    >>> invoice.invoice_date = today
    >>> line = InvoiceLine()
    >>> invoice.lines.append(line)
    >>> line.product = product
    >>> line.quantity = 5
    >>> len(line.taxes) == 1
    True
    >>> line.aeat340_book_key == rtype
    True
    >>> line.aeat340_operation_key == 'C'
    True
    >>> line.amount == Decimal(125)
    True
    >>> line = InvoiceLine()
    >>> invoice.lines.append(line)
    >>> len(line.taxes) == 0
    True
    >>> line.account = expense
    >>> line.description = 'Test 2'
    >>> line.quantity = 1
    >>> line.unit_price = Decimal(40)
    >>> tax = Tax(1)
    >>> line.taxes.append(tax)
    >>> line.aeat340_operation_key = 'C'
    >>> line.aeat340_book_key = utype
    >>> line.amount == Decimal(40)
    True
    >>> invoice.save()
    >>> Invoice.post([invoice.id], config.context)
    >>> (rec1, rec2) = Record.find([('invoice', '=', invoice.id)])
    >>> rec1.party_name == 'Party'
    True
    >>> rec1.party_nif == '00000000T'
    True
    >>> rec1.month == today.month
    True
    >>> rec1.book_key == 'U'
    True
    >>> rec1.operation_key == 'C'
    True
    >>> rec1.base == Decimal(165)
    True
    >>> rec1.tax_rate == Decimal(10)
    True
    >>> rec1.tax == Decimal(16.5)
    True
    >>> rec1.total == Decimal(181.5)
    True
    >>> rec2.book_key == 'R'
    True
    >>> rec2.operation_key == 'C'
    True
    >>> rec2.base == Decimal(165)
    True
    >>> rec2.tax_rate == Decimal(10)
    True
    >>> rec2.tax == Decimal(16.5)
    True
    >>> rec2.total == Decimal(181.5)
    True

Generate 340 Report::

    >>> Report = Model.get('aeat.340.report')
    >>> report = Report()
    >>> report.fiscalyear_code = 2013
    >>> report.period = "%02d" % (today.month)
    >>> report.company_vat = '123456789'
    >>> report.contact_name = 'Guido van Rosum'
    >>> report.contact_phone = '987654321'
    >>> report.representative_vat = '22334455'
    >>> report.save()
    >>> Report.calculate([report.id], config.context)
    >>> report.reload()
    >>> report.taxable_total == Decimal(810)
    True
    >>> report.sharetax_total == Decimal(81)
    True
    >>> report.total == Decimal(891)
    True
    >>> len(report.issued_lines) == 1
    True
    >>> len(report.received_lines) == 1
    True
    >>> len(report.investment_lines) == 1
    True
    >>> len(report.intracommunity_lines) == 1
    True
