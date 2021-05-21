Select
    x000v.VEND_BANK,
    Count(x000v.NUMBERS) As Count_NUMBERS
From
    X000_Vendor x000v
Group By
    x000v.VEND_BANK