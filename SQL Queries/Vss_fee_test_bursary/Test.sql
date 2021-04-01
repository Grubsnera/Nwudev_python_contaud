Select
    x001b.FINAIDCODE,
    x001b.QUAL_TYPE,
    Total(x001b.AMOUNT_TOTAL) As AMOUNT_TOTAL,
    Count(x001b.TRAN_COUNT) As STUD_COUNT
From
    X001aa_Bursary_student_value x001b
Where
    x001b.FINAIDCODE Is Not Null
Group By
    x001b.FINAIDCODE,
    x001b.QUAL_TYPE
Having
    Total(x001b.AMOUNT_TOTAL) <> 0 And
    Count(x001b.TRAN_COUNT) > 2