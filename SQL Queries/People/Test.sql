﻿Select
    X002_PEOPLE_CURR.MARITAL_STATUS,
    Count(X002_PEOPLE_CURR.DISABLED) As Count_DISABLED
From
    X002_PEOPLE_CURR
Group By
    X002_PEOPLE_CURR.MARITAL_STATUS
