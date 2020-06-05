Select
    X000_GL_trans.FDOC_NBR,
    Count(X000_GL_trans.CALC_AMOUNT) As AMT_COUNT,
    Total(X000_GL_trans.CALC_AMOUNT) As AMT_TOTAL
From
    X000_GL_trans
Group By
    X000_GL_trans.FDOC_NBR