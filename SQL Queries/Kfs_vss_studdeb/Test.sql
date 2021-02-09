Select
    Count(x002vpc.STUDENT) As Count_STUDENT,
    x002vpc.CAMPUS_CLOS,
    Total(x002vpc.BAL_CLOS) As Total_BAL_CLOS,
    x002vpc.CAMPUS_OPEN,
    Total(x002vpc.BAL_OPEN) As Total_BAL_OPEN,
    Total(x002vpc.DIFF_BAL) As Total_DIFF_BAL
From
    X002dc_vss_prevbal_curopen x002vpc
Where
    x002vpc.TYPE = 3
Group By
    x002vpc.CAMPUS_CLOS,
    x002vpc.CAMPUS_OPEN