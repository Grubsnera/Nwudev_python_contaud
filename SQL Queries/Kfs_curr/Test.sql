Select
    acc.VENDOR_ID,
    acc.VENDOR_NAME,
    Count(acc.EDOC) As Count_EDOC
From
    X001ad_Report_payments_accroute acc
Where
    acc.ACC_COST_STRING Like '%9641'
Group By
    acc.VENDOR_ID