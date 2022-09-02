Select
    ia_user.ia_user_name As Auditor,
    assi.ia_assi_year As Year,
    assi.ia_assi_file As FileRef,
    ia_assignment_category.ia_assicate_name As Category,
    ia_assignment_type.ia_assitype_name As Type,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=',assi.ia_assi_formedit,'&aid=',assi.ia_assi_auto,'&hash=',assi.ia_assi_token,'&category=',assi.ia_assicate_auto,'" target="_blank" rel="noopener noreferrer">',Concat(assi.ia_assi_name,' (', assi.ia_assi_auto, ')'),'</a>') As Assignment,
    Case
        When assi.ia_assi_priority = 1
        Then 'Low'
        When assi.ia_assi_priority = 2
        Then 'Medium'
        When assi.ia_assi_priority = 3
        Then 'High'
        When assi.ia_assi_priority = 4
        Then 'Follow-up'
        When assi.ia_assi_priority = 8
        Then 'Continuous'
        When assi.ia_assi_priority = 9
        Then 'Closed'
        Else 'Inactive'
    End As Priority,
    ia_assignment_status.ia_assistat_name As Status,
    Date(assi.ia_assi_startdate) As StartDate,
    Date(assi.ia_assi_completedate) As DueDate,
    Date(assi.ia_assi_finishdate) As CloseDate,
    Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=14" target="_blank" rel="noopener noreferrer">','Add','</a>',' | ','<a href = "index.php?option=com_rsform&view=rsform&formId=',assi.ia_assi_formedit,'&aid=',assi.ia_assi_auto,'&hash=',assi.ia_assi_token,'&category=',assi.ia_assicate_auto,'" target="_blank" rel="noopener noreferrer">','Edit', '</a>',' | ','<a href = "index.php?option=com_rsform&view=rsform&formId=',assi.ia_assi_formdelete,'&aid=',assi.ia_assi_auto,'&hash=',assi.ia_assi_token,'" target="_blank" rel="noopener noreferrer">','Delete','</a>', ' | ','<a href = "index.php?option=com_content&view=article&id=',assi.ia_assi_formview,'&hash=',assi.ia_assi_token,'" target="_blank" rel="noopener noreferrer">','Report','</a>') As Actions,
    Case
    When Count(find.ia_find_auto) > 1
    Then Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=9&aid=',assi.ia_assi_auto,'" target="_blank" rel="noopener noreferrer">Add</a>', '|','<a href = "https://www.ia-nwu.co.za/index.php?option=com_content&view=article&id=26&rid=',to_base64(Concat('1:', assi.ia_assi_auto)), '" target="_blank" rel="noopener noreferrer">',Concat(Cast(Count(find.ia_find_auto) As Character),'Findings'),'</a>')
    When Count(find.ia_find_auto) > 0
    Then Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=9&aid=',assi.ia_assi_auto,'" target="_blank" rel="noopener noreferrer">Add</a>','|','<a href = "https://www.ia-nwu.co.za/index.php?option=com_content&view=article&id=26&rid=',to_base64(Concat('1:', assi.ia_assi_auto)), '" target="_blank" rel="noopener noreferrer">',Concat(Cast(Count(find.ia_find_auto) As Character),'Finding'),'</a>')
    Else Concat('<a href = "https://www.ia-nwu.co.za/index.php?option=com_rsform&view=rsform&formId=9&aid=',assi.ia_assi_auto,'" target="_blank" rel="noopener noreferrer">Add</a>')
    End As Findings
From
    ia_assignment assi Left Join
    ia_assignment_category On ia_assignment_category.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_assignment_type On ia_assignment_type.ia_assitype_auto = assi.ia_assitype_auto Left Join
    ia_assignment_status On ia_assignment_status.ia_assistat_auto = assi.ia_assistat_auto Left Join
    ia_user On ia_user.ia_user_sysid = assi.ia_user_sysid Left Join
    ia_finding find On find.ia_assi_auto = assi.ia_assi_auto
Where
    ia_user.ia_user_name Like '%albert%'
Group By
    assi.ia_assi_auto