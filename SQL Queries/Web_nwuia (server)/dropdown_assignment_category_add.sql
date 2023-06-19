Select
    cate.ia_assicate_auto As value,
    cate.ia_assicate_name As label
From
    ia_assignment_category cate
Where
    -- cate.ia_assicate_customer = ".$customer_id." And
    cate.ia_assicate_customer = 1 And
    cate.ia_assicate_active = 1 And
    cate.ia_assicate_from <= Now() And
    cate.ia_assicate_to >= Now()
Order By
    label;