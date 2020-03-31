Select
    X002da_vss_student_balance_clos.CAMPUS,
    Total(X002da_vss_student_balance_clos.BALANCE) As Total_BALANCE
From
    X002da_vss_student_balance_clos
Group By
    X002da_vss_student_balance_clos.CAMPUS