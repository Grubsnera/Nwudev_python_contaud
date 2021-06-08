Select
    x004i.Tran_type,
    x004i.Tran_description,
    Sum(x004i.Amount_vss) As Sum_Amount_vss
From
    X004cx_invss_nogl x004i
Group By
    x004i.Tran_type,
    x004i.Tran_description