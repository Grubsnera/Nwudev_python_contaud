Select
    assi.ia_assi_si_caseyear As year,
    Max(assi.ia_assi_si_casenumber) As max
From
    ia_assignment assi
Where
    assi.ia_assi_si_caseyear = 2022
Group By
    assi.ia_assi_si_caseyear