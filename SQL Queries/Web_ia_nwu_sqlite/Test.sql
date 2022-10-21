Select
    user.ia_user_name As Auditor,
    assi.ia_assi_name As Assignment,
    assi.ia_assi_priority As Priority,
    ia_assignment_status.ia_assistat_name As Assignment_status,
    assi.ia_assi_proofdate As Report_date,
    Cast((StrfTime("%s", "now") - StrfTime("%s", assi.ia_assi_proofdate)) / 86400.0 As Integer) As Days_since_report,
    find.ia_find_name As Finding,
    reme.ia_findreme_name As Client,
    reme.ia_findreme_date_send As Send_date,
    Max(reme.ia_findreme_date_schedule) As Scedule_date
From
    ia_assignment assi Left Join
    ia_finding find On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_remediation reme On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid Inner Join
    ia_assignment_status On ia_assignment_status.ia_assistat_auto = assi.ia_assistat_auto
Where
    assi.ia_assi_priority = 4
Group By
    user.ia_user_name,
    assi.ia_assi_name,
    assi.ia_assi_priority,
    ia_assignment_status.ia_assistat_name,
    assi.ia_assi_proofdate,
    Cast((StrfTime("%s", "now") - StrfTime("%s", assi.ia_assi_proofdate)) / 86400.0 As Integer),
    find.ia_find_name,
    reme.ia_findreme_name,
    reme.ia_findreme_date_send
Order By
    Auditor,
    Report_date