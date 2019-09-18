Select
    X020_Student_feequal.PRESENT_CAT,
    X020_Student_feequal.ENROL_CAT,
    X020_Student_feequal.QUAL_TYPE,
    X020_Student_feequal.QUALIFICATION,
    X020_Student_feequal.FQUALLEVELAPID,
    Count(X020_Student_feequal.KSTUDBUSENTID) As Count_KSTUDBUSENTID,
    X020_Trans_feequal_mode.FEE_MODE
From
    X020_Student_feequal Left Join
    X020_Trans_feequal_mode On X020_Trans_feequal_mode.FQUALLEVELAPID = X020_Student_feequal.FQUALLEVELAPID
Group By
    X020_Student_feequal.FQUALLEVELAPID