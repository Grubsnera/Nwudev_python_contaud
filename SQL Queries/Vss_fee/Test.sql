Select
    Count(X010_Student_feereg.FEE_REG) As Count_FEE_REG,
    X010_Student_feereg.ENTRY_LEVEL
From
    X010_Student_feereg
Group By
    X010_Student_feereg.ENTRY_LEVEL