﻿Select
    x000u.USER_ID,
    Count(x000u.EMPLOYEE_NUMBER) As Count_EMPLOYEE_NUMBER
From
    X000_USER_CURR x000u
Group By
    x000u.USER_ID