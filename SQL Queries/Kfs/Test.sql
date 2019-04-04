Select
    X000_Vendor.PAYEE_ID_NBR,
    X000_Vendor.PAYEE_ID_TYP_CD,
    Count(X000_Vendor.BNK_ACCT_NBR) As Count_BNK_ACCT_NBR
From
    X000_Vendor
Group By
    X000_Vendor.PAYEE_ID_NBR,
    X000_Vendor.PAYEE_ID_TYP_CD
