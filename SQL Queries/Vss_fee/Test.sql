Select
    X020ba_Student_master.FEE_LEVIED_TYPE,
    Count(X020ba_Student_master.VALID) As Count_VALID
From
    X020ba_Student_master
Group By
    X020ba_Student_master.FEE_LEVIED_TYPE