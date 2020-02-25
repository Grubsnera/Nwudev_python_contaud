Select
    X021gx_qual_fee_overcharge.Campus,
    X021gx_qual_fee_overcharge.Tran_owner,
    Count(X021gx_qual_fee_overcharge.Student) As Count_Student
From
    X021gx_qual_fee_overcharge
Group By
    X021gx_qual_fee_overcharge.Campus,
    X021gx_qual_fee_overcharge.Tran_owner