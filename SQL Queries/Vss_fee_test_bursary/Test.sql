Select
    x001b.DIFF,
    Count(x001b.FFINAIDSITEID) As Count_FFINAIDSITEID
From
    X001ad_Bursary_student_compare x001b
Group By
    x001b.DIFF