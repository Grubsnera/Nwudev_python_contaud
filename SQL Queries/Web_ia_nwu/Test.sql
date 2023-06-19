Select
peop.employee_number,
peop.name_list,
peop.preferred_name,
peop.gender,
peop.phone_work,
peop.phone_mobile,
concat('<a href = "mailto: ', peop.email_address, '">', peop.email_address, '</a>') as email_address,
peop.organization,
peop.grade_calc,
peop.position_name
From
ia_people peop
Where
--peop.customer = '{user_param_1:cmd}'
peop.customer = 1
Group By
peop.employee_number,
peop.name_list;