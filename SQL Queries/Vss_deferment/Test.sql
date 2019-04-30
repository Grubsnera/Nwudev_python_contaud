Select
    X001ab_Students_deferment.DEFER_TYPE,
    X001ab_Students_deferment.DEFER_TYPE_DESC,
    X001ab_Students_deferment.FSITEORGUNITNUMBER,
    Count(X001ab_Students_deferment.KSTUDBUSENTID) As Sum_KSTUDBUSENTID,
    Sum(X001ab_Students_deferment.BAL_REG_CALC) As Sum_BAL_REG_CALC,
    Sum(X001ab_Students_deferment.BAL_DEF_CALC) As Sum_BAL_DEF_CALC,
    Sum(X001ab_Students_deferment.BAL_CUR) As Sum_BAL_CUR
From
    X001ab_Students_deferment
Group By
    X001ab_Students_deferment.DEFER_TYPE,
    X001ab_Students_deferment.DEFER_TYPE_DESC,
    X001ab_Students_deferment.FSITEORGUNITNUMBER
Order By
    X001ab_Students_deferment.FSITEORGUNITNUMBER,
    X001ab_Students_deferment.DEFER_TYPE
