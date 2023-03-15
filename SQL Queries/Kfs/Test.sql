Select
    X000_Vendor.VEND_BRANCH,
    X000_Vendor.VEND_BANK,
    Count(X000_Vendor.VNDR_INACTV_REAS_CD) As Count_VNDR_INACTV_REAS_CD
From
    X000_Vendor
Group By
    X000_Vendor.VEND_BRANCH,
    X000_Vendor.VEND_BANK