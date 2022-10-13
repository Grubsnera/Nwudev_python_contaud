Select
    iaac.ia_assicate_name As category,
    iaat.ia_assitype_name As type,
    iaas1.ia_assistat_name As status,
    Count(iaas.ia_assi_token) As assignments
From
    ia_assignment iaas Left Join
    ia_assignment_category iaac On iaac.ia_assicate_auto = iaas.ia_assicate_auto Left Join
    ia_assignment_type iaat On iaat.ia_assitype_auto = iaas.ia_assitype_auto Left Join
    ia_assignment_status iaas1 On iaas1.ia_assistat_auto = iaas.ia_assistat_auto
Where
    (iaas.ia_user_sysid = 855 And
        iaas.ia_assi_year = Year(Now())) Or
    (iaas.ia_user_sysid = 855 And
        iaas.ia_assi_year < Year(Now()) And
        iaas.ia_assi_priority < 9) Or
    (iaas.ia_user_sysid = 855 And
        Date(iaas.ia_assi_finishdate) >= Date_Sub(Concat(Year(Now()), '-10-01'), Interval 1 Year) And
        Date(iaas.ia_assi_finishdate) <= Date_Sub(Concat(Year(Now()), '-10-01'), Interval 1 Day))
Group By
    iaac.ia_assicate_name,
    iaat.ia_assitype_name,
    iaas1.ia_assistat_name