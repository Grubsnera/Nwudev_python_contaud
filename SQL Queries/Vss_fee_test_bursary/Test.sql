﻿Select
    x000t.STUDENT,
    x000t.FINAIDCODE,
    x000t.FINAIDNAAM,
    Total(x000t.AMOUNT) As Total_AMOUNT,
    Count(x000t.FFINAIDSITEID) As Count_FFINAIDSITEID
From
    X000_Transaction x000t
Group By
    x000t.STUDENT,
    x000t.FINAIDCODE,
    x000t.FINAIDNAAM