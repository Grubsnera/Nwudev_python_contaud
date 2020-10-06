Select
    X000_GL_trans.UNIV_FISCAL_YR,
    X000_GL_trans.UNIV_FISCAL_PRD_CD,
    Count(X000_GL_trans.FDOC_NBR) As Count_FDOC_NBR
From
    X000_GL_trans
Group By
    X000_GL_trans.UNIV_FISCAL_YR,
    X000_GL_trans.UNIV_FISCAL_PRD_CD