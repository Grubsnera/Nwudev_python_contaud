Select
    X001ad_Report_payments_accroute.PAYEE_TYPE,
    X001ad_Report_payments_accroute.PAYEE_TYP_DESC,
    X001ad_Report_payments_accroute.VENDOR_TYPE,
    X001ad_Report_payments_accroute.VENDOR_TYPE_CALC,
    X001ad_Report_payments_accroute.DOC_TYPE,
    Count(X001ad_Report_payments_accroute.EDOC) As Count_EDOC
From
    X001ad_Report_payments_accroute
Group By
    X001ad_Report_payments_accroute.PAYEE_TYPE,
    X001ad_Report_payments_accroute.PAYEE_TYP_DESC,
    X001ad_Report_payments_accroute.VENDOR_TYPE,
    X001ad_Report_payments_accroute.VENDOR_TYPE_CALC,
    X001ad_Report_payments_accroute.DOC_TYPE