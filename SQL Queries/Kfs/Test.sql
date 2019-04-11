Select
    X002aa_approver_last.DOC_HDR_ID,
    Count(X002aa_approver_last.PRNCPL_ID) As Count_PRNCPL_ID
From
    X002aa_approver_last
Group By
    X002aa_approver_last.DOC_HDR_ID
