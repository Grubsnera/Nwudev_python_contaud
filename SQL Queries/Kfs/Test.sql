Select
    X001ac_Report_payments_approute_curr.*
From
    X001ac_Report_payments_approute_curr
Where
    SubStr(X001ac_Report_payments_approute_curr.VENDOR_ID, 1, 8) =
    X001ac_Report_payments_approute_curr.APPROVE_EMP_NO