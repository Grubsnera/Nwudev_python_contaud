Select
    X007dab_employee_leave_code_invalid.ASSIGNMENT_CATEGORY,
    X007dab_employee_leave_code_invalid.EMPLOYEE_CATEGORY,
    X007dab_employee_leave_code_invalid.PERIOD,
    X007dab_employee_leave_code_invalid.WORKDAYS,
    X007dab_employee_leave_code_invalid.GRADE,
    X007dab_employee_leave_code_invalid.LEAVE_CODE,
    Count(X007dab_employee_leave_code_invalid.EMPLOYEE_NUMBER) As Total_EMPLOYEE_NUMBER
From
    X007dab_employee_leave_code_invalid
Group By
    X007dab_employee_leave_code_invalid.ASSIGNMENT_CATEGORY,
    X007dab_employee_leave_code_invalid.EMPLOYEE_CATEGORY,
    X007dab_employee_leave_code_invalid.PERIOD,
    X007dab_employee_leave_code_invalid.WORKDAYS,
    X007dab_employee_leave_code_invalid.GRADE,
    X007dab_employee_leave_code_invalid.LEAVE_CODE