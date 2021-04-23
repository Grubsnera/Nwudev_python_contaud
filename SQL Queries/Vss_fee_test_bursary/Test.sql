Select
    x001b.STUDENT,
    x001b.FINAIDCODE,
    x001b.FINAIDNAME,
    x001b.QUAL_TYPE,
    x001b.AMOUNT_TOTAL,
    x001b.TRAN_COUNT,
    x001b1.AMOUNT
From
    X001aa_Bursary_student_value x001b Left Join
    X001ac_Bursary_mode x001b1 On x001b1.FINAIDCODE = x001b.FINAIDCODE
            And x001b1.QUAL_TYPE = x001b.QUAL_TYPE
Where
    x001b.FINAIDCODE Is Not Null And
    x001b.QUAL_TYPE Is Not Null And
    x001b.AMOUNT_TOTAL <> 0