Select
    dir.nwu_number,
    dir.employee_name,
    dir.national_identifier,
    dir.date_submitted,
    dir.registration_number,
    dir.company_name,
    dir.enterprise_type,
    dir.company_status,
    dir.business_start_date,
    dir.directorship_start_date
From
    ia_director dir
Where
    -- dir.customer = '{user_param_1:cmd}'
    dir.customer = '1'
Group By
    dir.nwu_number,
    dir.registration_number