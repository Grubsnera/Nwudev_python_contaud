Select
    r.nwu_number,
    r.employee_name,
    r.national_identifier,
    Max(r.date_submitted) As Date_submitted,
    r.registration_number,
    r.company_name,
    r.enterprise_type,
    r.company_status,
    r.history_date,
    r.business_start_date,
    r.directorship_status,
    r.directorship_start_date,
    r.directorship_end_date,
    r.directorship_type,
    r.directorship_interest,
    r.nationality
From
    X004e_searchworks_results_import r
Group By
    r.nwu_number,
    r.national_identifier,
    r.registration_number
Order By
    r.nwu_number,
    r.business_start_date