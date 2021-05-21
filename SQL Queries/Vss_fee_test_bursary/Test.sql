Select
    x000t.CAMPUS,
    Total(x000t.AMOUNT) As Total_AMOUNT
From
    X000_Transaction x000t
Group By
    x000t.CAMPUS