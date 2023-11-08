Select
    v.VNDR_NM
From
    X000_Vendor v
Where
    v.VNDR_TYP_CD In ('PO', 'DV') And
    v.DOBJ_MAINT_CD_ACTV_IND = 'Y'