Select
    STUD.PRESENT_CAT,
    STUD.QUAL_TYPE,
    Sum(MODU.Sum_S0) As S0,
    Sum(MODU.Sum_S1) As S1,
    Sum(MODU.Sum_S2) As S2,
    Sum(MODU.Sum_S3) As S3,
    Sum(MODU.Sum_S4) As S4,
    Sum(MODU.Sum_S5) As S5,
    Sum(MODU.Sum_S6) As S6,
    Sum(MODU.Sum_S7) As S7,
    Sum(MODU.Sum_S8) As S8,
    Sum(MODU.Sum_S9) As S9
From
    X020ba_Student_master STUD Inner Join
    X020ad_Student_module_summ MODU On MODU.KSTUDBUSENTID = STUD.KSTUDBUSENTID
            And MODU.FQUALLEVELAPID = STUD.FQUALLEVELAPID
Group By
    STUD.PRESENT_CAT,
    STUD.QUAL_TYPE