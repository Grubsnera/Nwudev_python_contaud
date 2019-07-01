Select
    X001ad_Report_payments_initroute_curr.DOC_TYPE,
    Count(X001ad_Report_payments_initroute_curr.DISB_NBR) As Count_DISB_NBR
From
    X001ad_Report_payments_initroute_curr
Group By
    X001ad_Report_payments_initroute_curr.DOC_TYPE