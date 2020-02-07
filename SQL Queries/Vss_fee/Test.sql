Select
    STUD.ORGUNIT_NAME As Faculty,
    STUD.CAMPUS As Campus,
    STUD.FEE_LEVIED_TYPE As Fee_type,
    STUD.PRESENT_CAT As Present_category,
    STUD.ENROL_CAT As Enrol_category,
    Count(STUD.KSTUDBUSENTID) As Student_count,
    Sum(STUD.FEE_LEVIED) As Total_income
From
    X020ba_Student_master STUD
Group By
    STUD.ORGUNIT_NAME,
    STUD.CAMPUS,
    STUD.FEE_LEVIED_TYPE,
    STUD.PRESENT_CAT,
    STUD.ENROL_CAT