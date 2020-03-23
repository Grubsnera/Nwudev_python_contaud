Select
    X000_Vendor.VENDOR_ID,
    X000_Vendor.VNDR_NM,
    X000_Vendor.VNDR_TYP_CD,
    Lower(X000_Vendor.VEND_MAIL) As VEND_MAIL,
    Lower(X000_Vendor.EMAIL) As VEND_MAIL2,
    Lower(X000_Vendor.EMAIL_CONTACT) As VEND_MAILC
From
    X000_Vendor
Where
    (Lower(X000_Vendor.VEND_MAIL) Like ('%nwu.ac.za%') And
        X000_Vendor.DOBJ_MAINT_CD_ACTV_IND = 'Y') Or
    (Lower(X000_Vendor.EMAIL) Like ('%nwu.ac.za%') And
        X000_Vendor.DOBJ_MAINT_CD_ACTV_IND = 'Y') Or
    (Lower(X000_Vendor.EMAIL_CONTACT) Like ('%nwu.ac.za%') And
        X000_Vendor.DOBJ_MAINT_CD_ACTV_IND = 'Y')