Select
    emp.employee,
    emp.name_address,
    emp.phone,
    ven.VENDOR_ID,
    ven.PAYEE_NAME,
    ven.OWNER_TYPE_DESC,
    ven.LAST_PMT_DT,
    ven.NET_PMT_AMT,
    ven.TRAN_COUNT,
    ven.NUMBERS,
    SubStr(emp.phone, -9) As test
From
    X100_phone_emp emp,
    X100_phone_vend ven
Where
    ven.NUMBERS Like ('%' || SubStr(emp.phone || '%', -9))