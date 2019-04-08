Select
    PUR_VNDR_DTL_T.VNDR_ID As VENDOR_ID,
    PUR_VNDR_DTL_T.VNDR_NM,
    PUR_VNDR_DTL_T.VNDR_URL_ADDR,
    PUR_VNDR_HDR_T.VNDR_TAX_NBR,
    X001ad_vendor_bankacc.VEND_BANK,
    X001ad_vendor_bankacc.EMPL_BANK,
    X001ad_vendor_bankacc.STUD_BANK,
    X001ac_vendor_address_comb.FAX,
    X001ac_vendor_address_comb.EMAIL,
    X001ac_vendor_address_comb.ADDRESS,
    X001ac_vendor_address_comb.URL,
    X001ac_vendor_address_comb.STATE_CD,
    X001ac_vendor_address_comb.COUNTRY_CD,
    PUR_VNDR_HDR_T.VNDR_TAX_TYP_CD,
    PUR_VNDR_HDR_T.VNDR_TYP_CD,
    PUR_VNDR_DTL_T.VNDR_PMT_TERM_CD,
    PUR_VNDR_DTL_T.VNDR_SHP_TTL_CD,
    PUR_VNDR_DTL_T.VNDR_PARENT_IND,
    PUR_VNDR_DTL_T.VNDR_1ST_LST_NM_IND,
    PUR_VNDR_DTL_T.COLLECT_TAX_IND,
    PUR_VNDR_HDR_T.VNDR_FRGN_IND,
    PUR_VNDR_DTL_T.VNDR_CNFM_IND,
    PUR_VNDR_DTL_T.VNDR_PRPYMT_IND,
    PUR_VNDR_DTL_T.VNDR_CCRD_IND,
    PUR_VNDR_DTL_T.DOBJ_MAINT_CD_ACTV_IND,
    PUR_VNDR_DTL_T.VNDR_INACTV_REAS_CD
From
    PUR_VNDR_DTL_T Left Join
    PUR_VNDR_HDR_T On PUR_VNDR_HDR_T.VNDR_HDR_GNRTD_ID = PUR_VNDR_DTL_T.VNDR_HDR_GNRTD_ID Left Join
    X001ac_vendor_address_comb On X001ac_vendor_address_comb.VENDOR_ID = PUR_VNDR_DTL_T.VNDR_ID Left Join
    X001ad_vendor_bankacc On X001ad_vendor_bankacc.VENDOR_ID = PUR_VNDR_DTL_T.VNDR_ID
