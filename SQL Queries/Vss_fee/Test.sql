Select
    X020_Student_feequal.VALID,
    X020_Student_feequal.PRESENT_CAT,
    X020_Student_feequal.FSITEORGUNITNUMBER,
    Count(X020_Student_feequal.KSTUDBUSENTID) As Count_KSTUDBUSENTID,
    X020_Student_feequal.FEE_TYPE,
    X020_Student_feequal.FEE_CAT
From
    X020_Student_feequal
Group By
    X020_Student_feequal.VALID,
    X020_Student_feequal.PRESENT_CAT,
    X020_Student_feequal.FSITEORGUNITNUMBER,
    X020_Student_feequal.FEE_TYPE,
    X020_Student_feequal.FEE_CAT