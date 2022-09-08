Select
    papf.employee_number,
    papf.name_address,
    papf.marital_status,
    ppei.person_extra_info_id,
    ppei.person_id,
    ppei.spouse_number,
    ppei.spouse_title,
    ppei.spouse_initials,
    ppei.spouse_name_last,
    ppei.spouse_date_of_birth,
    ppei.spouse_national_identifier,
    ppei.spouse_passport,
    ppei.spouse_start_date,
    ppei.spouse_end_date,
    ppei.spouse_create_date,
    ppei.spouse_created_by,
    ppei.spouse_update_date,
    ppei.spouse_updated_by,
    ppei.spouse_update_login,
    strftime('%Y', 'now', '-1 year')||'-12-31' - strftime('%Y-%m-%d', ppei.spouse_date_of_birth) as spouse_age
From
    X000_SPOUSE ppei Inner Join
    X000_PEOPLE papf On papf.person_id = ppei.person_id
Where
    StrfTime('%Y-%m-%d', 'now') Between ppei.spouse_start_date And IfNull(ppei.spouse_end_date, '4712-12-31')