Select
    x010s.FBUSENTID,
    x010s.AMOUNT,
    x010s.TRANSCODE,
    x010s.FDEBTCOLLECTIONSITE,
    x010s.TRANSDATE
From
    X010_Studytrans x010s
Where
    (x010s.TRANSCODE = '214' And
        x010s.FDEBTCOLLECTIONSITE = -1 And
        x010s.TRANSDATE Like ('2021-01%')) Or
    (x010s.TRANSCODE = '214' And
        x010s.FDEBTCOLLECTIONSITE = -1 And
        x010s.TRANSDATE Like ('2021-02%'))