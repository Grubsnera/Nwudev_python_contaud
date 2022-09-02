Select
    reme.ia_findreme_auto,
    Concat('<a href = "https://www.ia-nwu.co.za/index.php/menu-finding/form/3/', assi.ia_assi_auto, '">',
    Concat(assi.ia_assi_name, " (", assi.ia_assi_auto, ")"), '</a>') As ia_assi_name,
    Concat('<a href = "https://www.ia-nwu.co.za/index.php/menu-finding/form/41/', reme.ia_find_auto, '">',
    find.ia_find_name, '</a>') As ia_find_name,
    reme.ia_findreme_name,
    reme.ia_findreme_date_schedule,
    reme.ia_findreme_date_send,
    reme.ia_findreme_date_submit,
    iafr.ia_findresp_name,
    Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&formId=', reme.ia_findreme_formedit, '&id=',
    reme.ia_findreme_auto, '">Edit</a>', " | ",
    '<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&formId=', reme.ia_findreme_formdelete, '&id=',
    reme.ia_findreme_auto, '">Delete</a>', " | ",
    '<a href = "https://www.ia-nwu.co.za/index.php?option=com_content&view=article&id=', reme.ia_findreme_formview,
    '&hash=', reme.ia_findreme_token, '">View</a>', " | ",
    '<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&formId=', reme.ia_findreme_formtransfer, '&id=',
    reme.ia_findreme_auto, '">Update</a>') As ia_findreme_actions
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response iafr On iafr.ia_findresp_auto = reme.ia_findresp_auto
Where
    assi.ia_user_sysid = 855 And
    reme.ia_findreme_mail_trigger > 0 And
    reme.ia_findreme_schedule > 0 And
    reme.ia_findreme_date_schedule <= Now()
Order By
    ia_assi_name,
    ia_find_name,
    reme.ia_findreme_name,
    reme.ia_findreme_date_schedule