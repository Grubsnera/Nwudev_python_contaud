﻿Select
    user.ia_user_name As auditor,
    assi.ia_assi_year As year,
    Concat(assi.ia_assi_name, ' (', assi.ia_assi_auto, ')') As assignment,
    Concat(find.ia_find_name, ' (', find.ia_find_auto, ')') As finding,
    fist.ia_findstat_name As status,
    Concat(rate.ia_findrate_name, ' (', rate.ia_findrate_impact, ')') As rating,
    Concat(likh.ia_findlike_name, ' (', likh.ia_findlike_value, ')') As likelihood,
    Concat(cont.ia_findcont_name, ' (', cont.ia_findcont_value, ')') As control_effectiveness,
    Concat('<a href = "index.php?option=com_content&view=article&id=', find.ia_find_formview, '&rid=',
    find.ia_find_auto, '" target="_blank" rel="noopener noreferrer">View</a>') As actions,
    Case
        When Count(reme.ia_findreme_auto) > 1
        Then Concat(Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'Requests'))
        When Count(reme.ia_findreme_auto) > 0
        Then Concat(Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'Request'))
    End As remediation
From
    ia_finding find Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto Left Join
    ia_finding_remediation reme On reme.ia_find_auto = find.ia_find_auto Left Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid Left Join
    ia_finding_rate rate On rate.ia_findrate_auto = find.ia_findrate_auto Left Join
    ia_finding_control cont On cont.ia_findcont_auto = find.ia_findcont_auto Left Join
    ia_finding_likelihood likh On likh.ia_findlike_auto = find.ia_findlike_auto
Where
    (assi.ia_assi_priority < 9 And find.ia_find_auto > 0) Or
    (assi.ia_assi_year = Year(Now()) And find.ia_find_auto > 0) Or
    (Date(assi.ia_assi_finishdate) > Date_Sub(Concat(Year(Now()), '-10-01'), Interval 1 Year) And find.ia_find_auto > 0)
Group By
    find.ia_find_auto
Order By
    auditor,
    year,
    assignment,
    finding