Select
    ia_user.ia_user_name,
    ia_assignment.ia_assi_year,
    ia_assignment.ia_assi_name,
    ia_assignment_type.ia_assitype_name,
    ia_assignment.ia_assi_priority,
    ia_assignment_status.ia_assistat_name,
    ia_assignment.ia_assi_startdate,
    ia_assignment.ia_assi_completedate,
    ia_assignment.ia_assi_finishdate,
    ia_assignment.ia_assi_desc
From
    ia_assignment Left Join
    ia_user On ia_user.ia_user_sysid = ia_assignment.ia_user_sysid Left Join
    ia_assignment_type On ia_assignment_type.ia_assitype_auto = ia_assignment.ia_assitype_auto Left Join
    ia_assignment_status On ia_assignment_status.ia_assistat_auto = ia_assignment.ia_assistat_auto