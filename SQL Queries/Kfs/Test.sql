Select
    X001aa_Report_payments.PAYEE_ID,
    Count(X001aa_Report_payments.EDOC) As Count_EDOC
From
    X001aa_Report_payments
Where
    X001aa_Report_payments.DOC_TYPE = 'CDV'
Group By
    X001aa_Report_payments.PAYEE_ID,
    X001aa_Report_payments.DOC_TYPE