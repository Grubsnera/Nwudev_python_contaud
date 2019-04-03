Select
    X005_paye_master.NUMB,
    Count(X005_paye_master.NAT) As Count_NAT
From
    X005_paye_master
Group By
    X005_paye_master.NUMB
