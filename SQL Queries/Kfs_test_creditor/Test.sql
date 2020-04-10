Select
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,
    a.VENDOR_ID,
    a.VENDOR_TYPE,
    a.DOC_TYPE,
    a.PMT_DT,
    a.EDOC,
    a.TOT_AMOUNT,
    b.PMT_DT As PMT_DT1,
    cast(julianday(b.PMT_DT) - julianday(a.PMT_DT) As int) As DAYS,
    b.EDOC As EDOC1,
    b.TRAN_COUNT,
    b.TOT_AMOUNT As TOT_AMOUNT1,
    cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) As TOTAL
From
    X001baa_Small_split_pay_summ a Inner Join
    X001bab_Small_split_pay_list b On b.ORG_NM = a.ORG_NM
            And b.FIN_OBJ_CD_NM = a.FIN_OBJ_CD_NM
            And b.VENDOR_ID = a.VENDOR_ID
            And julianday(b.PMT_DT) - julianday(a.PMT_DT) < 3
            And julianday(b.PMT_DT) - julianday(a.PMT_DT) > 0
            And cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) > 5000
            And a.FIN_OBJ_CD_NM Not Like ('%TRAVEL%')