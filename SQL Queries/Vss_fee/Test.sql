Select
    X020bx_Student_master_sort.PRESENT_CAT,
    Count(X020bx_Student_master_sort.KSTUDBUSENTID) As Count_KSTUDBUSENTID
From
    X020bx_Student_master_sort
Group By
    X020bx_Student_master_sort.PRESENT_CAT