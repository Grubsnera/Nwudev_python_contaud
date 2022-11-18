Select
    burs.student,
    burs.total_burs,
    burs.total_loan,
    burs.total_external,
    burs.total_internal,
    burs.total_research,
    burs.total_trust,
    burs.total_other,
    burs.staff_discount,
    burs.active,
    burs.levy_category,
    burs.qualification,
    burs.qualification_type,
    burs.discontinue_date,
    burs.discontinue_result,
    burs.discontinue_reason
From
    X001_Bursary_summary_student burs
Where
    (burs.total_burs <> 0 And
        burs.active = 'INACTIVE') Or
    (burs.total_burs <> 0 And
        burs.discontinue_date Is Not Null)