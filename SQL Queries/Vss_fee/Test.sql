Select
    X020ba_Student_master.FQUALLEVELAPID,
    X020ba_Student_master.CAMPUS,
    X020ba_Student_master.PRESENT_CAT,
    X020ba_Student_master.ENROL_CAT,
    X020ba_Student_master.QUALIFICATION,
    X020ba_Student_master.QUALIFICATION_NAME,
    Count(X020ba_Student_master.VALID) As COUNT
From
    X020ba_Student_master
Where
    X020ba_Student_master.FEE_LEVIED_TYPE = '1 NO TRANS/ZERO FEE' And
    X020ba_Student_master.FEE_SHOULD_BE Like ('4%')
Group By
    X020ba_Student_master.FQUALLEVELAPID,
    X020ba_Student_master.CAMPUS,
    X020ba_Student_master.PRESENT_CAT,
    X020ba_Student_master.ENROL_CAT,
    X020ba_Student_master.QUALIFICATION,
    X020ba_Student_master.QUALIFICATION_NAME,
    X020ba_Student_master.FEE_LEVIED_TYPE,
    X020ba_Student_master.FEE_SHOULD_BE