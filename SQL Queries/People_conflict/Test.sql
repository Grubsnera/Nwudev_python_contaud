Select
    X100_bank_emp.ORG,
    X100_bank_emp.LOC,
    X100_bank_emp.EMP,
    X100_bank_emp.ACC_TYPE,
    X100_bank_emp.ACC_BRANCH,
    X100_bank_emp.ACC_NUMBER,
    X100_bank_emp.ACC_RELATION,
    X100_bank_ven.VENDOR_ID,
    X100_bank_ven.VENDOR_NAME,
    X100_bank_ven.VENDOR_BANK,
    X001_declarations_curr.DECLARATION_DATE,
    X001_declarations_curr.STATUS
From
    X100_bank_emp Inner Join
    X100_bank_ven On X100_bank_ven.VENDOR_BANK = X100_bank_emp.ACC_NUMBER Left Join
    X001_declarations_curr On X001_declarations_curr.EMPLOYEE = X100_bank_emp.EMP
