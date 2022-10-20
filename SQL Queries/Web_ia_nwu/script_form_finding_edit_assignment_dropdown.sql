﻿Select
    assi.ia_assi_auto,
    Case
        When assi.ia_assi_permission = '855'
        Then Concat('|Foreign| ', cate.ia_assicate_name, '  ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ')', ' (',
            typy.ia_assitype_name, ')')
        Else Concat(cate.ia_assicate_name, '  ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ')', ' (',
            typy.ia_assitype_name, ')')
    End As ia_assi_namenumb
From
    ia_assignment assi Inner Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
    ia_assignment_type typy On typy.ia_assitype_auto = assi.ia_assitype_auto
Where
    (assi.ia_user_sysid = '855') Or
    (assi.ia_assi_permission = '855')
Order By
    cate.ia_assicate_name,
    assi.ia_assi_editdate Desc