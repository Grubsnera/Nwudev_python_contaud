Select
    X001aa_Report_payments.PAYEE_TYPE,
    X001aa_Report_payments.PAYEE_TYPE2,
    X001aa_Report_payments.VENDOR_TYPE,
    X001aa_Report_payments.DOC_TYPE,
    X001aa_Report_payments.DOC_LABEL,
    Count(X001aa_Report_payments.EDOC) As Count_EDOC
From
    X001aa_Report_payments
Group By
    X001aa_Report_payments.PAYEE_TYPE,
    X001aa_Report_payments.PAYEE_TYPE2,
    X001aa_Report_payments.VENDOR_TYPE,
    X001aa_Report_payments.DOC_TYPE,
    X001aa_Report_payments.DOC_LABEL