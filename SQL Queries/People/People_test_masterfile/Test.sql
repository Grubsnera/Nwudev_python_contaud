Select
    X007da_leave.EMPLOYEE_NUMBER,
    Count(X007da_leave.VALID) As Count_VALID
From
    X007da_leave
Group By
    X007da_leave.EMPLOYEE_NUMBER
Having
    Count(X007da_leave.VALID) > 1