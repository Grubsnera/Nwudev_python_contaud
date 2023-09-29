Select
    find.ia_assi_auto,
    find.ia_find_name,
    find.ia_find_createdate,
    find.ia_find_private,
    find.ia_findstat_auto,
    find.ia_find_note,
    find.ia_find_desc_toggle,
    find.ia_find_desc,
    find.ia_find_risk_toggle,
    find.ia_find_risk,
    find.ia_find_criteria_toggle,
    find.ia_find_criteria,
    find.ia_find_procedure_toggle,
    find.ia_find_procedure,
    find.ia_find_condition_toggle,
    find.ia_find_condition,
    find.ia_find_effect_toggle,
    find.ia_find_effect,
    find.ia_find_cause_toggle,
    find.ia_find_cause,
    find.ia_find_recommend_toggle,
    find.ia_find_recommend,
    find.ia_find_comment_toggle,
    find.ia_find_comment,
    find.ia_find_frequency_toggle,
    find.ia_find_frequency,
    find.ia_find_definition_toggle,
    find.ia_find_definition,
    find.ia_find_reference_toggle,
    find.ia_find_reference,
    find.ia_findlike_auto,
    flik.ia_findlike_desc,
    find.ia_findrate_auto,
    frat.ia_findrate_desc,
    find.ia_findcont_auto,
    fcon.ia_findcont_desc,
    find.ia_findlike_auto_client,
    clik.ia_findlike_desc As ia_findlike_desc_client,
    find.ia_findrate_auto_client,
    crat.ia_findrate_desc As ia_findrate_desc_client,
    find.ia_findcont_auto_client,
    ccon.ia_findcont_desc As ia_findrcont_desc_client
From
    ia_finding find Left Join
    ia_finding_likelihood flik On flik.ia_findlike_auto = find.ia_findlike_auto Left Join
    ia_finding_rate frat On frat.ia_findrate_auto = find.ia_findrate_auto Left Join
    ia_finding_control fcon On fcon.ia_findcont_auto = find.ia_findcont_auto Left Join
    ia_finding_likelihood clik On clik.ia_findlike_auto = find.ia_findlike_auto_client Left Join
    ia_finding_rate crat On crat.ia_findrate_auto = find.ia_findrate_auto_client Left Join
    ia_finding_control ccon On ccon.ia_findcont_auto = find.ia_findcont_auto_client
Where
    find.ia_find_auto = 906 And
    find.ia_find_token = '3e8830b63d7e55194d54e9682c4f6292'