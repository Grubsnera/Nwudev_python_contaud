Select
    TYPE.STUDENT,
    Count(TYPE.BAL_CLOS) As Total_BAL_CLOS,
    Total(TYPE.BAL_OPEN) As Total_BAL_OPEN,
    Total(TYPE.DIFF_BAL) As Total_DIFF_BAL
From
    X002dd_vss_closing_open_differ TYPE
Group By
    TYPE.STUDENT