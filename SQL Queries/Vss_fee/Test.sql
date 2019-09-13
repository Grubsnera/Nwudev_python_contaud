Select
    X020_Student_feequal.FEE_TYPE,
    X020_Student_feequal.PRESENT_CAT,
    Count(X020_Student_feequal.FEE_CALC) As Count_FEE_CALC,
    Total(X020_Student_feequal.FEE_QUAL) As Total_FEE_QUAL,
    Total(X020_Student_feequal.FEE_MODE) As Total_FEE_MODE
From
    X020_Student_feequal
Group By
    X020_Student_feequal.FEE_TYPE,
    X020_Student_feequal.PRESENT_CAT