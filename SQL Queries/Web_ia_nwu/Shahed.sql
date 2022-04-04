Select
    `as`.ia_user_sysid,
    `as`.ia_assi_auto,
    `as`.ia_assi_name,
    `as`.ia_assicond_auto,
    co.ia_assicond_name,
    `as`.ia_assicate_auto,
    ca.ia_assicate_name,
    `as`.ia_assitype_auto,
    ty.ia_assitype_name,
    `as`.ia_assiorig_auto,
    `or`.ia_assiorig_name,
    `as`.ia_assistat_auto,
    ia_assignment_status.ia_assistat_name
From
    ia_assignment `as` Left Join
    ia_assignment_category ca On ca.ia_assicate_auto = `as`.ia_assicate_auto Left Join
    ia_assignment_type ty On ty.ia_assitype_auto = `as`.ia_assitype_auto Left Join
    ia_assignment_conducted co On co.ia_assicond_auto = `as`.ia_assicond_auto Left Join
    ia_assignment_origin `or` On `or`.ia_assiorig_auto = `as`.ia_assiorig_auto Left Join
    ia_assignment_status On ia_assignment_status.ia_assistat_auto = `as`.ia_assistat_auto
Where
    `as`.ia_user_sysid = 876