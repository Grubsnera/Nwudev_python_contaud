﻿Select
    x004b.KFINAIDSITEID,
    x004b.FFINAIDID As Count_FFINAIDID,
    Count(x004b.FSITEORGUNITNUMBER) As Count_FSITEORGUNITNUMBER
From
    X004_Bursaries x004b
Group By
    x004b.KFINAIDSITEID,
    x004b.FFINAIDID