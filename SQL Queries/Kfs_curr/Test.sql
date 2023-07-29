Select
    venc.VENDOR_ID As vendor_id,
    venc.VENDOR_NAME As vendor_name,
    Count(venc.EDOC) As tran_count,
    Max(venc.PMT_DT) As last_pay_date,
    Total(venc.NET_PMT_AMT) As tran_total
From
    X001ad_Report_payments_accroute venc
Where
    venc.VENDOR_TYPE_CALC In ('DV', 'PO') And
    SubStr(venc.ACC_COST_STRING, -4) Not In ('2552', '2553')
Group By
    venc.VENDOR_ID