Select
    X001_people_id_master.IDNO,
    Count(X001_people_id_master.EMPLOYEE_NUMBER) As COUNT
From
    X001_people_id_master
Group By
    X001_people_id_master.IDNO
