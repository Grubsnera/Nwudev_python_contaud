Select
    X020aa_Fiabd007_summ.FQUALLEVELAPID,
    Count(X020aa_Fiabd007_summ.AMOUNT) As Count_AMOUNT
From
    X020aa_Fiabd007_summ
Group By
    X020aa_Fiabd007_summ.FQUALLEVELAPID