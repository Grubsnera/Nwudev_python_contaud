Select
    peop.position_id As position1,
    peop.position_name As position_name1,
    peop.employee_number As employee1,
    peop.full_name As name1,
    peop.user_person_type As type1,
    peop.employee_category As category1,
    post.POS02 As position2,
    peop2.position_name As position_name2,
    peop2.employee_number As employee2,
    peop2.full_name As name2,
    peop.division,
    peop.faculty,
    peop.supervisor_number,
    peop.supervisor_name,
    peop.oe_code,
    peop.organization,
    peop.org_head_person_id,
    peop.oe_head_number,
    peop.oe_head_name_name
From
    X999_PEOPLE_CURR peop Left Join
    X000_POS_STRUCT_10 post On post.POS01 = peop.position_id Left Join
    X999_PEOPLE_CURR peop2 On peop2.position_id = post.POS02
Order By
    employee2,
    position2,
    position1