Select
    STUD.*
From
    X020ba_Student_master STUD
Where
    STUD.VALID = 0 And
    STUD.FEE_LEVIED_TYPE Like ('1%')
Order By
    STUD.CAMPUS,
    STUD.FEE_SHOULD_BE