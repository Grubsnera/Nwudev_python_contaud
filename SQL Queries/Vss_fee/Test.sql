Select
    X020bx_Student_master_sort.KSTUDBUSENTID,
    Count(X020bx_Student_master_sort.VALID) As Count_VALID
From
    X020bx_Student_master_sort
Where
    X020bx_Student_master_sort.FEE_LEVIED_TYPE Like ('4%')
Group By
    X020bx_Student_master_sort.KSTUDBUSENTID
Having
    Count(X020bx_Student_master_sort.VALID) > 1