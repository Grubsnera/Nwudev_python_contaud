Select
    X007_grade_leave_master.EMPLOYMENT_CATEGORY,
    X007_grade_leave_master.PERSON_TYPE,
    X007_grade_leave_master.ACAD_SUPP,
    X007_grade_leave_master.GRADE,
    Count(X007_grade_leave_master.GRADE_CALC) As Count_GRADE_CALC
From
    X007_grade_leave_master
Group By
    X007_grade_leave_master.EMPLOYMENT_CATEGORY,
    X007_grade_leave_master.PERSON_TYPE,
    X007_grade_leave_master.ACAD_SUPP,
    X007_grade_leave_master.GRADE