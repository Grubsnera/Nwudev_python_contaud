﻿Select
    X002_PEOPLE_PREV_YEAR.LEAVING_REASON,
    X002_PEOPLE_PREV_YEAR.LEAVE_REASON_DESCRIP,
    Count(X002_PEOPLE_PREV_YEAR.ASS_ID) As Count_ASS_ID
From
    X002_PEOPLE_PREV_YEAR
Group By
    X002_PEOPLE_PREV_YEAR.LEAVING_REASON,
    X002_PEOPLE_PREV_YEAR.LEAVE_REASON_DESCRIP