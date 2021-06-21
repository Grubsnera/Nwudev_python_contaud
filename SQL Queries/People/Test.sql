Select
    x999p.employee_number,
    Count(x999p.parent_position_id) As Count_parent_position_id
From
    X999_PEOPLE_CURR x999p
Group By
    x999p.employee_number