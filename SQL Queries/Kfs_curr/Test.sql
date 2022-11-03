Select
    X001aa_Report_payments.VENDOR_TYPE_CALC,
    Total(X001aa_Report_payments.NET_PMT_AMT) As Total_NET_PMT_AMT
From
    X001aa_Report_payments
Where
    X001aa_Report_payments.NET_PMT_AMT <= 5000
Group By
    X001aa_Report_payments.VENDOR_TYPE_CALC