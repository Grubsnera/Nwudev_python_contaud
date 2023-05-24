Select
    X010_Studytrans.FDEBTCOLLECTIONSITE,
    Total(X010_Studytrans.AMOUNT) As Total_AMOUNT
From
    X010_Studytrans
Where
    X010_Studytrans.TRANSCODE Is Not Null
Group By
    X010_Studytrans.FDEBTCOLLECTIONSITE