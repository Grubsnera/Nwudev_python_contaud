Select
    PAYM.ORG_NM,
    PAYM.EDOC,
    PAYM.VENDOR_ID,
    PAYM.VENDOR_TYPE,
    PAYM.PMT_DT,
    Total(PAYM.ACC_AMOUNT) As Total_ACC_AMOUNT
From
    X001ad_Report_payments_accroute PAYM
Where
    PAYM.VENDOR_TYPE = 'V'
Group By
    PAYM.ORG_NM,
    PAYM.EDOC,
    PAYM.VENDOR_ID,
    PAYM.VENDOR_TYPE,
    PAYM.PMT_DT
Having
    Total(PAYM.ACC_AMOUNT) > 2500 And
    Total(PAYM.ACC_AMOUNT) < 5000
Order By
    PAYM.ORG_NM,
    PAYM.VENDOR_ID,
    PAYM.PMT_DT