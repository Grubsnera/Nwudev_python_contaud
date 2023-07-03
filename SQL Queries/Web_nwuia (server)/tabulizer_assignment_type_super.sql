﻿Select
    concat(cont.name, ' - ', cate.ia_assicate_name) As cust_cate,
    type.ia_assitype_file As file,
    type.ia_assitype_name As name,
    type.ia_assitype_desc As description,
    Date(type.ia_assitype_from) As date_from,
    Date(type.ia_assitype_to) As date_to,
    Case
        When type.ia_assitype_active = 1
        Then 'Yes'
        Else 'No'
    End As active,
    Concat('<a href = "index.php?option=com_rsform&formId=', type.ia_assitype_form, '&recordId=', type.ia_assitype_auto,
    '&action=edit">Edit</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=', type.ia_assitype_form, '&recordId=',
    type.ia_assitype_auto, '&action=copy">Copy</a>', ' | ', '<a href = "index.php?option=com_rsform&formId=',
    type.ia_assitype_form, '&recordId=', type.ia_assitype_auto, '&action=delete">Delete</a>') As actions
From
    ia_assignment_type type Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = type.ia_assicate_auto Inner Join
    jm4_contact_details cont On cont.id = type.ia_assitype_customer
Group By
    concat(cont.name, ' - ', cate.ia_assicate_name),
    type.ia_assitype_name