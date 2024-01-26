﻿Select
    user.ia_user_name As Auditor,
    assi.ia_assi_year As Year,
    cate.ia_assicate_name As Category,
    type.ia_assitype_name As Type,
    Concat(assi.ia_assi_name, ' (', assi.ia_assi_auto, ')') As Assignment,
    Case
        When assi.ia_assi_priority = 1
        Then 'Low'
        When assi.ia_assi_priority = 2
        Then 'Medium'
        When assi.ia_assi_priority = 3
        Then 'High'
        When assi.ia_assi_priority = 7
        Then 'Follow-up'
        When assi.ia_assi_priority = 8
        Then 'Continuous'
        When assi.ia_assi_priority = 9
        Then 'Closed'
        Else 'Inactive'
    End As Priority,
    stat.ia_assistat_name As Status,
    assi.ia_assi_startdate As Start_Date,
    assi.ia_assi_completedate As Due_Date,
    assi.ia_assi_finishdate As Close_Date,
    assi.ia_assi_offi As Notes_Official,
    assi.ia_assi_desc As Notes_Own,
    assi.ia_assi_auto As Aid,
    Concat(assi.ia_assi_year, '.', assi.ia_assi_file) As File_Reference,
    assi.ia_assi_editdate
From
    ia_assignment assi Left Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Left Join
    ia_assignment_status stat On stat.ia_assistat_auto = assi.ia_assistat_auto Left Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid
Where
    Case
        When Month(Date(Now())) In ('10', '11', '12')
        Then (assi.ia_user_sysid = 855 And assi.ia_assi_year = Year(Date_Add(Now(), Interval 1 Year)) And
            user.ia_user_active = '1' And cate.ia_assicate_private >= '0') Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_year < Year(Date_Add(Now(), Interval 1 Year)) And
            user.ia_user_active = '1' And assi.ia_assi_priority < 9 And cate.ia_assicate_private >= '0') Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_finishdate Between Concat(Year(Date(Now())), '1001') And
            Concat(Year(Date_Add(Now(), Interval 1 Year)), '0930') And user.ia_user_active = '1' And
            cate.ia_assicate_private >= '0')
        Else (assi.ia_user_sysid = 855 And assi.ia_assi_year = Year(Date(Now())) And user.ia_user_active = '1'
            And cate.ia_assicate_private >= '0') Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_year < Year(Date(Now())) And user.ia_user_active = '1' And
            assi.ia_assi_priority < 9 And cate.ia_assicate_private >= '0') Or
            (assi.ia_user_sysid = 855 And assi.ia_assi_finishdate Between Concat(Year(Date_Add(Now(), Interval
            -1 Year)), '1001') And Concat(Year(Date(Now())), '0930') And user.ia_user_active = '1' And
            cate.ia_assicate_private >= '0')
    End
Group By
    cate.ia_assicate_name,
    type.ia_assitype_name,
    assi.ia_assi_auto,
    assi.ia_assi_name