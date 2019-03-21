Select
    X005bd_paye_addprev.ORG,
    X005bd_paye_addprev.LOC,
    X005bd_paye_addprev.EMP,
    X005bd_paye_addprev.NUMB
From
    X005bd_paye_addprev Left Join
    X005bf_offi On X005bf_offi.EMPLOYEE_NUMBER = X005bd_paye_addprev.NUMB
        And X005bf_offi.CAMPUS = X005bd_paye_addprev.LOC
Where
    X005bf_offi.LOOKUP = 'TEST_PAYE_INVALID_OFFICER'
