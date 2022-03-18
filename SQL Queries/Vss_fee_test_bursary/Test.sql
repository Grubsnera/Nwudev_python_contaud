    Select
    BU.STUDENT,
    ST.EMP_NAME_FULL,
    ST.EMP_PERSON_TYPE,
    BU.FINAIDCODE,
    BU.FINAIDNAME,
    BU.LEVY_CATEGORY,
    BU.AMOUNT_TOTAL,
    SD.TRAN_VALUE AS STAFF_DISC
From
    X001_Bursary_student_value BU Inner Join
    X001_Student_employee ST On ST.KSTUDBUSENTID = BU.STUDENT Left Join
    X001_Bursary_student_value_staffdisc SD On SD.STUDENT = BU.STUDENT