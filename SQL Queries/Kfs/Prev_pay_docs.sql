Select
    X001_Report_payments_prev.*,
    X000_Documents.DOC_TYP_NM,
    X000_Documents.LBL
From
    X001_Report_payments_prev Left Join
    X000_Documents On X000_Documents.DOC_HDR_ID = X001_Report_payments_prev.CUST_PMT_DOC_NBR
