Select
    iafi.ia_find_auto,
    iafi.ia_find_name,
    iaas.ia_assi_auto,
    iaas.ia_assi_name,
    iaas.ia_assi_year,
    iaas.ia_assi_priority,
    iaas1.ia_assistat_name,
    iaas.ia_assi_finishdate,
    iafs.ia_findstat_name,
    iafc.ia_findcont_name,
    iafc.ia_findcont_value,
    iafr.ia_findrate_name,
    iafr.ia_findrate_impact,
    iafl.ia_findlike_name,
    iafl.ia_findlike_value
From
    ia_finding iafi Inner Join
    ia_assignment iaas On iaas.ia_assi_auto = iafi.ia_assi_auto Inner Join
    ia_finding_status iafs On iafs.ia_findstat_auto = iafi.ia_findstat_auto Left Join
    ia_finding_control iafc On iafc.ia_findcont_auto = iafi.ia_findcont_auto Left Join
    ia_finding_rate iafr On iafr.ia_findrate_auto = iafi.ia_findrate_auto Left Join
    ia_finding_likelihood iafl On iafl.ia_findlike_auto = iafi.ia_findlike_auto Left Join
    ia_assignment_status iaas1 On iaas1.ia_assistat_auto = iaas.ia_assistat_auto
Where
    (iaas.ia_assi_year = 2022) Or
    (iaas.ia_assi_year = 2021 And
        iaas.ia_assi_priority < 9) Or
    (iaas.ia_assi_finishdate >= '2021-10-01' And
        iaas.ia_assi_finishdate <= '2022-09-30')