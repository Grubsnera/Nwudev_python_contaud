Select
    orig.ia_assiorig_auto As value,
    orig.ia_assiorig_name As label
From
    ia_assignment_origin orig
Where
    -- orig.ia_assiorig_customer = ".$customer_id." And
    orig.ia_assiorig_customer = 1 And
    orig.ia_assiorig_active = 1 And
    orig.ia_assiorig_from <= Now() And
    orig.ia_assiorig_to >= Now()
Order By
    label;