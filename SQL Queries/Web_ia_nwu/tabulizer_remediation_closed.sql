Select
    reme.ia_findreme_auto,
    Concat(assi.ia_assi_name, " (", assi.ia_assi_auto, ")") As ia_assi_name,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=',find.ia_find_formedit,'&fid=',find.ia_find_auto,'&hash=',find.ia_find_token,'" target="_blank" rel="noopener noreferrer">',find.ia_find_name,'</a>') As ia_find_name,
    reme.ia_findreme_name,
    reme.ia_findreme_date_send,
    reme.ia_findreme_date_submit,
    reme.ia_findreme_date_update,
    iafr.ia_findresp_name,
    Concat('<a href = "index.php?option=com_rsform&formId=',reme.ia_findreme_formedit,'&id=',reme.ia_findreme_auto,'" target="_blank" rel="noopener noreferrer">Edit</a>'," | ",'<a href = "index.php?option=com_rsform&formId=',reme.ia_findreme_formdelete,'&id=',reme.ia_findreme_auto, '" target="_blank" rel="noopener noreferrer">Delete</a>', " | ",'<a href = "index.php?option=com_content&view=article&id=',reme.ia_findreme_formview,'&hash=',reme.ia_findreme_token,'" target="_blank" rel="noopener noreferrer">View</a>') As ia_findreme_actions
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_response iafr On iafr.ia_findresp_auto = reme.ia_findresp_auto
Where
    assi.ia_user_sysid = 855 And
    reme.ia_findreme_mail_trigger = 0
Order By
    ia_assi_name,
    ia_find_name,
    reme.ia_findreme_name,
    reme.ia_findreme_date_send Desc