Select
    X000_Vendor.VNDR_TYP_CD,
    Count(X000_Vendor.VENDOR_ID) As Count_VENDOR_ID
From
    X000_Vendor
Where
    X000_Vendor.DOBJ_MAINT_CD_ACTV_IND = 'Y'
Group By
    X000_Vendor.VNDR_TYP_CD,
    X000_Vendor.DOBJ_MAINT_CD_ACTV_IND