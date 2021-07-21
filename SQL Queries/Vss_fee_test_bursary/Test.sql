Select
    a.STUDENT,
    a.BURS_COUNT,
    a.AMOUNT_TOTAL,
    x001b.FINAIDCODE,
    x001b.FINAIDNAME,
    x001b.QUAL_TYPE,
    x001b.AMOUNT_TOTAL As AMOUNT_BURSARY,
    x001b.TRAN_COUNT
From
    X001aa_Bursary_student_value_total a Inner Join
    X001aa_Bursary_student_value x001b On a.STUDENT = x001b.STUDENT
Where
    a.AMOUNT_TOTAL < -200000 And
    x001b.AMOUNT_TOTAL != 0