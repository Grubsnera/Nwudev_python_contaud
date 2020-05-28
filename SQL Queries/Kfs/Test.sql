Select
    X000_GL_trans.CALC_COST_STRING,
    X000_GL_trans.ORG_NM,
    X000_GL_trans.ACCOUNT_NM,
    X000_GL_trans.FIN_OBJ_CD_NM,
    Max(X000_GL_trans.TRANSACTION_DT) As Max_TRANSACTION_DT,
    Total(X000_GL_trans.CALC_AMOUNT) As Total_CALC_AMOUNT
From
    X000_GL_trans
Group By
    X000_GL_trans.CALC_COST_STRING,
    X000_GL_trans.ORG_NM,
    X000_GL_trans.ACCOUNT_NM,
    X000_GL_trans.FIN_OBJ_CD_NM
Order By
    X000_GL_trans.ORG_NM,
    X000_GL_trans.ACCOUNT_NM,
    X000_GL_trans.FIN_OBJ_CD_NM