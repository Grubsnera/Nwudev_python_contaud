Select
    X002ab_vss_transort.CAMPUS_VSS,
    X002ab_vss_transort.TRANSDATE_VSS,
    Sum(X002ab_vss_transort.AMOUNT_VSS) As Sum_AMOUNT_VSS,
    Sum(X002ab_vss_transort.AMOUNT_DT) As Sum_AMOUNT_DT,
    Sum(X002ab_vss_transort.AMOUNT_CR) As Sum_AMOUNT_CR
From
    X002ab_vss_transort
Where
    X002ab_vss_transort.MONTH_VSS = '05'
Group By
    X002ab_vss_transort.CAMPUS_VSS,
    X002ab_vss_transort.TRANSDATE_VSS