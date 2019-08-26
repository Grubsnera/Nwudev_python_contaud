Select
    X001ae_Report_payments_accroute_curr.PAYEE_NAME,
    X001ae_Report_payments_accroute_curr.ORG_NM,
    X001ae_Report_payments_accroute_curr.VENDOR_TYPE,
    X001ae_Report_payments_accroute_curr.DOC_TYPE,
    Sum(X001ae_Report_payments_accroute_curr.ACC_AMOUNT) As Sum_ACC_AMOUNT
From
    X001ae_Report_payments_accroute_curr
Where
    X001ae_Report_payments_accroute_curr.VENDOR_TYPE = 'V'
Group By
    X001ae_Report_payments_accroute_curr.PAYEE_NAME,
    X001ae_Report_payments_accroute_curr.ORG_NM,
    X001ae_Report_payments_accroute_curr.VENDOR_TYPE,
    X001ae_Report_payments_accroute_curr.DOC_TYPE