Select
    X020ae_Student_convert.KSTUDBUSENTID,
    Count(X020ae_Student_convert.FQUALLEVELAPID) As Count_FQUALLEVELAPID
From
    X020ae_Student_convert
Group By
    X020ae_Student_convert.KSTUDBUSENTID