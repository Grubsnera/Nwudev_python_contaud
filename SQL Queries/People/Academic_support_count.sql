﻿Select
    x002p.ACAD_SUPP,
    Count(x002p.ASS_ID) As ACAD_SUPP_COUNT
From
    X002_PEOPLE_CURR x002p
Group By
    x002p.ACAD_SUPP