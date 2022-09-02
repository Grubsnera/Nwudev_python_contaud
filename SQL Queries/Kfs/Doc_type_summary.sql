Select
    SubStr(X001aa_Report_payments.VENDOR_ID, 1, 8) As VENDOR_ID,
    X001aa_Report_payments.PAYEE_NAME As VENDOR_NAME,
    X001aa_Report_payments.VENDOR_TYPE_CALC As VENDOR_TYPE,
    X001aa_Report_payments.DOC_TYPE,
    X001aa_Report_payments.DOC_LABEL,
    Max(X001aa_Report_payments.DISB_TS) As DISB_DT,
    Count(X001aa_Report_payments.EDOC) As TRAN_COUNT,
    Total(X001aa_Report_payments.NET_PMT_AMT) As TRAN_TOTAL
From
    X001aa_Report_payments
Where
    X001aa_Report_payments.DISB_TS >= "2020-06-01" And
    X001aa_Report_payments.DISB_TS <= "2020-06-30"
Group By
    SubStr(X001aa_Report_payments.VENDOR_ID, 1, 8),
    X001aa_Report_payments.DOC_TYPE
Order By
    TRAN_TOTAL Desc