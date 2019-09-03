Select
    X010_Student_feereg.FEE_TYPE,
    Count(X010_Student_feereg.FBUSINESSENTITYID) As COUNT,
    Total(X010_Student_feereg.FEE_REG) As TOTAL
From
    X010_Student_feereg
Group By
    X010_Student_feereg.FEE_TYPE