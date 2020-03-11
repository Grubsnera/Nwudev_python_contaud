Select
    X001ad_Report_payments_accroute.*,
    X001ad_Report_payments_accroute.VENDOR_ID As VENDOR_ID1,
    X001ad_Report_payments_accroute.INIT_EMP_NAME As INIT_EMP_NAME1
From
    X001ad_Report_payments_accroute
Where
    X001ad_Report_payments_accroute.VENDOR_ID = '11274298-0' And
    X001ad_Report_payments_accroute.INIT_EMP_NAME Like ('%PYB VAN BLERK%')