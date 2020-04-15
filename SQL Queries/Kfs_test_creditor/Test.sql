Select
    X001bac_Small_split_pay_select.ACC_COST_STRING,
    X001bac_Small_split_pay_select.EDOC_A,
    X001bac_Small_split_pay_select.AMOUNT_PD_A,
    X001bac_Small_split_pay_select.EDOC_B,
    X001bac_Small_split_pay_select.AMOUNT_PD_B,
    Count(X001bac_Small_split_pay_select.DAYS_AFTER) As Count_DAYS_AFTER
From
    X001bac_Small_split_pay_select
Group By
    X001bac_Small_split_pay_select.ACC_COST_STRING,
    X001bac_Small_split_pay_select.EDOC_A,
    X001bac_Small_split_pay_select.AMOUNT_PD_A,
    X001bac_Small_split_pay_select.EDOC_B,
    X001bac_Small_split_pay_select.AMOUNT_PD_B