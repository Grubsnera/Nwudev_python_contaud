Select
    Concat(cate.ia_assicate_name, ' (', type.ia_assitype_name, ') ', assi.ia_assi_name, ' (', assi.ia_assi_auto,
    ')') As assignment,
    user.ia_user_name As owner,
    Concat('<a title="Finding edit" href="index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit,
    '&recordId=', find.ia_find_auto, '&recordHash=', find.ia_find_token, '&action=edit&assignment=', find.ia_assi_auto,
    '" target="_blank" rel="noopener nofollow noreferrer">', Concat(find.ia_find_name, ' (', find.ia_find_auto, ')'),
    '</a>') As Finding,
    Concat(Date(find.ia_find_editdate), ' ', Substr(MonthName(find.ia_find_editdate), 1, 3)) As dateedit,
    fist.ia_findstat_name As status,
    Case
        -- When assi.ia_assi_permission = {user_id}
        When assi.ia_assi_permission = 855
        Then Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit, '&fid=',
            find.ia_find_auto, '&hash=', find.ia_find_token, '" target="_blank" rel="noopener noreferrer">Edit</a>',
            ' | ', '<a href = "index.php?option=com_content&view=article&id=', find.ia_find_formview, '&rid=',
            find.ia_find_auto, '" target="_blank" rel="noopener noreferrer">View</a>')
        Else Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit, '&fid=',
            find.ia_find_auto, '&hash=', find.ia_find_token, '" target="_blank" rel="noopener noreferrer">Edit</a>',
            ' | ', '<a href = "index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formdelete, '&fid=',
            find.ia_find_auto, '&hash=', find.ia_find_token, '" target="_blank" rel="noopener noreferrer">Delete</a>',
            ' | ', '<a href = "index.php?option=com_content&view=article&id=', find.ia_find_formview, '&rid=',
            find.ia_find_auto, '" target="_blank" rel="noopener noreferrer">View</a>')
    End As actions,
    Case
        When Count(reme.ia_findreme_auto) > 1
        Then Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=3">Add</a>', '|',
            Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'requests'))
        When Count(reme.ia_findreme_auto) > 0
        Then Concat('<a href = "index.php?option=com_rsform&view=rsform&formId=3">Add</a>', '|',
            Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'request'))
        Else '<a href = "index.php?option=com_rsform&view=rsform&formId=3">Add</a>'
    End As remediation
From
    ia_finding find Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto Left Join
    ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto Left Join
    ia_finding_remediation reme On reme.ia_find_auto = find.ia_find_auto Left Join
    ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto Left Join
    ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Left Join
    ia_user user On user.ia_user_sysid = assi.ia_user_sysid
Where
    -- find.ia_assi_auto = {user_param_1:int}
    find.ia_assi_auto = 564
Group By
    cate.ia_assicate_name,
    type.ia_assitype_name,
    assi.ia_assi_name,
    find.ia_find_name,
    find.ia_find_auto