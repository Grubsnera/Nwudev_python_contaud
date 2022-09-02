Select
    assi.ia_assi_auto,
    case
    when ia_assi_permission = '855' then Concat('Foreign - ',assi.ia_assi_name,' (',assi.ia_assi_auto,')')
    else
    Concat('Own - ',assi.ia_assi_name,' (',assi.ia_assi_auto,')')
    end as ia_assi_namenumb
From
    ia_assignment assi
Where
    (assi.ia_user_sysid = '855') Or
    (assi.ia_assi_permission = '855')
Order By
    assi.ia_assi_editdate Desc