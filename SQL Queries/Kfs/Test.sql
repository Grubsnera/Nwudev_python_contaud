Select
    Count(x000v.NUMBERS) As Count_NUMBERS,
    x000v.VNDR_TYP_CD
From
    X000_Vendor x000v
Group By
    x000v.VNDR_TYP_CD