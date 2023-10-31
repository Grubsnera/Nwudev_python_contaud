﻿Select
    reme.ia_findreme_auto,
    reme.ia_find_auto,
    reme.ia_findreme_response,
    reme.ia_findrate_auto,
    concat(frat.ia_findrate_name,' - ',frat.ia_findrate_desc) As ia_findrate_name,
    reme.ia_findlike_auto,
    concat(flik.ia_findlike_name,' - ',flik.ia_findlike_desc) As ia_findlike_name,
    reme.ia_findcont_auto,
    concat(fcon.ia_findcont_name,' - ',fcon.ia_findcont_desc) As ia_findcont_name,
    find.ia_find_comment,
    reme.ia_findreme_name,
    reme.ia_findreme_date_submit,
    reme.ia_findreme_mail_trigger
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_finding_likelihood flik On flik.ia_findlike_auto = reme.ia_findlike_auto Inner Join
    ia_finding_rate frat On frat.ia_findrate_auto = reme.ia_findrate_auto Inner Join
    ia_finding_control fcon On fcon.ia_findcont_auto = reme.ia_findcont_auto
Where
    -- reme.ia_findreme_auto = '".$id."'
    reme.ia_findreme_auto = 1214