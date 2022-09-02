Select
    assi.ia_assi_auto,
    Concat("(", find.ia_assi_auto, ") ", assi.ia_assi_name) As ia_assi_namenumb
From
    ia_assignment assi Inner Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto Inner Join
    ia_finding_status On ia_finding_status.ia_findstat_auto = find.ia_findstat_auto
Where
    assi.ia_user_sysid = 855 And
    assi.ia_assi_priority < 9 And
    ia_finding_status.ia_findstat_name = 'Request remediation'
Group By
    assi.ia_assi_auto,
    assi.ia_assi_name
Order By
    assi.ia_assi_name