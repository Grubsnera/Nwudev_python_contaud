Select
    X020aa_Fiabd007.FQUALLEVELAPID,
    X020aa_Fiabd007.AMOUNT,
    Count(X020aa_Fiabd007.UMPT_REGU) As Count_UMPT_REGU
From
    X020aa_Fiabd007
Group By
    X020aa_Fiabd007.FQUALLEVELAPID,
    X020aa_Fiabd007.AMOUNT