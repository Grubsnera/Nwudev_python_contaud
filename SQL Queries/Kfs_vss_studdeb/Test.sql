Select
    X002dc_vss_prevbal_curopen.TYPE,
    Total(X002dc_vss_prevbal_curopen.BAL_CLOS) As Total_BAL_CLOS,
    Total(X002dc_vss_prevbal_curopen.BAL_OPEN) As Total_BAL_OPEN,
    Total(X002dc_vss_prevbal_curopen.DIFF_BAL) As Total_DIFF_BAL,
    Count(X002dc_vss_prevbal_curopen.STUDENT) As Count_STUDENT
From
    X002dc_vss_prevbal_curopen
Group By
    X002dc_vss_prevbal_curopen.TYPE