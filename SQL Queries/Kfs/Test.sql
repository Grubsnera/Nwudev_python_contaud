Select
    X001aa_Report_payments_curr.PAYEE_TYP_DESC,
    Count(X001aa_Report_payments_curr.DISB_NBR) As Count_DISB_NBR
From
    X001aa_Report_payments_curr
Group By
    X001aa_Report_payments_curr.PAYEE_TYP_DESC
