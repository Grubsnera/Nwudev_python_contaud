Select
    X001aa_Report_payments_prev.CUST_PMT_DOC_NBR,
    Count(X001aa_Report_payments_prev.REQS_NBR) As Count_REQS_NBR
From
    X001aa_Report_payments_prev
Group By
    X001aa_Report_payments_prev.CUST_PMT_DOC_NBR
