Select
    X001aa_Report_payments.EDOC,
    X001aa_Report_payments.PO_NBR,
    X001aa_Report_payments.INV_NBR,
    X001aa_Report_payments.INV_DT,
    X001aa_Report_payments.ORIG_INV_AMT,
    X001aa_Report_payments.NET_PMT_AMT
From
    X001aa_Report_payments
Where
    X001aa_Report_payments.PO_NBR != ""