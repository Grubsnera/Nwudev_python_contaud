Select
    aprt.FDOC_NBR,
    Count(aprt.PO_ID) As Count_PO_ID
From
    AP_PMT_RQST_T aprt
Group By
    aprt.FDOC_NBR