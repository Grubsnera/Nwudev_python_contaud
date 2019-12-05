Select
    X007dx_leavecode_invalid.Person_type,
    Count(X007dx_leavecode_invalid.Workdays) As Count_Workdays
From
    X007dx_leavecode_invalid
Group By
    X007dx_leavecode_invalid.Person_type