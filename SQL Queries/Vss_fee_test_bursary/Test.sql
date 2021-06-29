Select
    b.STUDENT,
    b.BURS_COUNT,
    b.AMOUNT_TOTAL,
    b.TRAN_COUNT,
    s.KSTUDBUSENTID As STUDENT_REGISTERED,
    e.STUDENT As STUDENT_EMPLOYEE,
    x000s.STUDENT As STUDENT_STAFF_DISC
From
    X001aa_Bursary_student_value_total b Left Join
    X000_Student s On s.KSTUDBUSENTID = b.STUDENT Left Join
    X000_Student_employee e On e.STUDENT = b.STUDENT Left Join
    X000_Student_staff x000s On x000s.STUDENT = b.STUDENT