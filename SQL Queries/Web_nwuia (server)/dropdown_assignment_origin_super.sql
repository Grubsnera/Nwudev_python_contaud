Select
    orig.ia_assiorig_auto As value,
    case
    when orig.ia_assiorig_active = 1 then concat(cont.name, ' - ', orig.ia_assiorig_name, ' (Actve) ', date(orig.ia_assiorig_from), '/', date(orig.ia_assiorig_to))
    else concat(cont.name, ' - ', orig.ia_assiorig_name, ' (InActve) ', date(orig.ia_assiorig_from), '/', date(orig.ia_assiorig_to))
    end as label
From
    ia_assignment_origin orig Inner Join
    jm4_contact_details cont On cont.id = orig.ia_assiorig_customer
Order By
    label