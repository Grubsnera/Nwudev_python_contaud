Select
    x002vt.TRANSCODE_VSS,
    x002vt.TRANSDATE_VSS,
    Total(x002vt.AMOUNT_VSS) As Total_AMOUNT_VSS
From
    X002ab_vss_transort x002vt
Where
    x002vt.TRANSCODE_VSS = '061'
Group By
    x002vt.TRANSCODE_VSS,
    x002vt.TRANSDATE_VSS