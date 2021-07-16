Select
    x000p.employee_number,
    x000p.name_full,
    x000p.preferred_name,
    SubStr(x000p.name_last, 1, 2) As name_last,
    x000p.employee_age
From
    X000_PEOPLE x000p
Where
    SubStr(x000p.name_last, 1, 2) >= 'JA' And
    SubStr(x000p.name_last, 1, 2) <= 'LI' And
    x000p.employee_age >= 35 And
    x000p.location Like ('POT%')