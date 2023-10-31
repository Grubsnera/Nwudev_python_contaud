Select
    Concat(cate.ia_assicate_name, ' (', type.ia_assitype_name, ') ', assi.ia_assi_name, ' (', assi.ia_assi_auto, ')') As assignment,
    user.ia_user_name As owner,
    Concat('<a title="Finding edit" href="index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit, '&recordId=', find.ia_find_auto, '&recordHash=', find.ia_find_token, '&action=edit&assignment=', find.ia_assi_auto, '" target="_blank" rel="noopener nofollow noreferrer">', Concat(find.ia_find_name, ' (', find.ia_find_auto, ')'), '</a>') As Finding,
    Concat(Date(find.ia_find_editdate), ' ', Substr(MonthName(find.ia_find_editdate), 1, 3)) As dateedit,
    fist.ia_findstat_name As status,
    Case
        -- When assi.ia_assi_permission = {user_id}
        When assi.ia_assi_permission = 855
        Then Concat('<a title="Finding report" href = "index.php?option=com_content&view=article&id=', find.ia_find_formview, '&hash=', find.ia_find_token, '" target="_blank" rel="noopener noreferrer nofollow">', 'View', '</a>')
        Else Concat('<a title="Finding copy" href="index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit, '&recordId=', find.ia_find_auto, '&recordHash=', find.ia_find_token, '&action=copy&assignment=', find.ia_assi_auto, '" target="_blank" rel="noopener nofollow noreferrer">Copy</a>', ' | ', '<a title="Finding delete" href="index.php?option=com_rsform&view=rsform&formId=', find.ia_find_formedit, '&recordId=', find.ia_find_auto, '&recordHash=', find.ia_find_token, '&action=delete&assignment=', find.ia_assi_auto, '" target="_blank" rel="noopener nofollow noreferrer">Delete</a>', ' | ', '<a title="Finding report" href = "index.php?option=com_content&view=article&id=', find.ia_find_formview, '&hash=', find.ia_find_token, '" target="_blank" rel="noopener noreferrer nofollow">', 'View', '</a>')
    End As actions,
    Case
        When (Count(reme.ia_findreme_auto) > 1 And fist.ia_findstat_name = 'Send for approval') Or (Count(reme.ia_findreme_auto) > 0 And fist.ia_findstat_name = 'Request remediation')
        Then Concat('<a title="Remediation add" href="index.php?option=com_rsform&view=rsform&formId=21', '&recordId=0', '&recordHash=0', '&action=add&assignment=', find.ia_assi_auto, '&finding=', find.ia_find_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Add', '</a>', ' | ', '<a title="Remediations" href = "index.php?option=com_content&view=article&id=32&fid=', to_base64(Concat('1:', find.ia_find_auto)), '" target="_blank" rel="noopener noreferrer nofollow">', Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'requests'), '</a>')
        When Count(reme.ia_findreme_auto) > 1
        Then Concat('<a title="Remediations" href = "index.php?option=com_content&view=article&id=32&fid=', to_base64(Concat('1:', find.ia_find_auto)), '" target="_blank" rel="noopener noreferrer nofollow">', Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'requests'), '</a>')
        When (Count(reme.ia_findreme_auto) > 0 And fist.ia_findstat_name = 'Send for approval') Or (Count(reme.ia_findreme_auto) > 0 And fist.ia_findstat_name = 'Request remediation')
        Then Concat('<a title="Remediation add" href="index.php?option=com_rsform&view=rsform&formId=21', '&recordId=0', '&recordHash=0', '&action=add&assignment=', find.ia_assi_auto, '&finding=', find.ia_find_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Add', '</a>', ' | ', '<a title="Remediations" href = "index.php?option=com_content&view=article&id=32&fid=', to_base64(Concat('1:', find.ia_find_auto)), '" target="_blank" rel="noopener noreferrer nofollow">', Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'request'), '</a>')
        When Count(reme.ia_findreme_auto) > 0
        Then Concat('<a title="Remediations" href = "index.php?option=com_content&view=article&id=32&fid=', to_base64(Concat('1:', find.ia_find_auto)), '" target="_blank" rel="noopener noreferrer nofollow">', Concat(Cast(Count(reme.ia_findreme_auto) As Character), 'requests'), '</a>')
        When (fist.ia_findstat_name = 'Send for approval') Or (fist.ia_findstat_name = 'Request remediation')
        Then Concat('<a title="Remediation add" href="index.php?option=com_rsform&view=rsform&formId=21', '&recordId=0', '&recordHash=0', '&action=add&assignment=', find.ia_assi_auto, '&finding=', find.ia_find_auto, '" target="_blank" rel="noopener nofollow noreferrer">', 'Add', '</a>')
        Else ''
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
    (find.ia_find_auto > 0 And
        -- assi.ia_user_sysid = {user_id} And
        assi.ia_user_sysid = 855 And
        assi.ia_assi_priority < 9) Or
    (find.ia_find_auto > 0 And
        assi.ia_assi_priority < 9 And
        -- assi.ia_assi_permission = {user_id})
        assi.ia_assi_permission = 855)
Group By
    cate.ia_assicate_name,
    type.ia_assitype_name,
    assi.ia_assi_name,
    find.ia_find_name,
    find.ia_find_auto