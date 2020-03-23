Select
    X000_Payments.VENDOR_ID,
    Max(X000_Payments.PMT_DT) As Max_PMT_DT,
    Total(X000_Payments.NET_PMT_AMT) As Total_NET_PMT_AMT
From
    X000_Payments
Group By
    X000_Payments.VENDOR_ID