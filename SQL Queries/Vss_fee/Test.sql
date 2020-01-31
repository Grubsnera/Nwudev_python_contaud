Select
    X021aa_Qual_nofee_loaded.FINDING,
    Count(X021aa_Qual_nofee_loaded.KSTUDBUSENTID) As Count_KSTUDBUSENTID
From
    X021aa_Qual_nofee_loaded
Group By
    X021aa_Qual_nofee_loaded.FINDING