Select
    ia_assignment_category.ia_assicate_auto,
    ia_assignment_category.ia_assicate_name,
    ia_assignment_category.ia_assicate_private,
    ia_assignment_type.ia_assitype_auto,
    ia_assignment_type.ia_assitype_name,
    ia_assignment_type.ia_assitype_private
From
    ia_assignment_category Inner Join
    ia_assignment_type On ia_assignment_type.ia_assicate_auto = ia_assignment_category.ia_assicate_auto