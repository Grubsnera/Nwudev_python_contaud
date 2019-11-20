Select
    X020ae_Student_convert.KSTUDBUSENTID,
    X020ae_Student_convert1.FQUALLEVELAPID,
    X020ae_Student_convert1.CONV_IND
From
    X020ae_Student_convert Inner Join
    X020ae_Student_convert X020ae_Student_convert1 On X020ae_Student_convert1.KSTUDBUSENTID =
            X020ae_Student_convert.KSTUDBUSENTID
Group By
    X020ae_Student_convert.KSTUDBUSENTID,
    X020ae_Student_convert1.FQUALLEVELAPID,
    X020ae_Student_convert1.CONV_IND
Having
    Count(X020ae_Student_convert.FQUALLEVELAPID) = 2