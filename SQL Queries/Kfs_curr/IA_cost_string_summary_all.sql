﻿Select
    GL.ACCOUNT_NM,
    GL.CALC_COST_STRING,
    GL.FIN_OBJ_CD_NM,
    GL.FIN_BALANCE_TYP_CD,
    Count(GL.FDOC_NBR) As Count_FDOC_NBR,
    Total(GL.CALC_AMOUNT) As Total_CALC_AMOUNT
From
    X000_GL_trans GL
Where
    GL.ACCOUNT_NM Like ("(4532%")
Group By
    GL.ACCOUNT_NM,
    GL.CALC_COST_STRING,
    GL.FIN_OBJ_CD_NM,
    GL.FIN_BALANCE_TYP_CD
Order By
    GL.ACCOUNT_NM,
    GL.FIN_OBJ_CD_NM