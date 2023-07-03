﻿ Select
    FIAB.FQUALLEVELAPID,
    Upper(FIAB.CAMPUS) As CAMPUS,
    FIAB.FPRESENTATIONCATEGORYCODEID,
    Upper(FIAB.PRESENT_CAT) As PRESENT_CAT,
    FIAB.FENROLMENTCATEGORYCODEID,
    Upper(FIAB.ENROL_CATEGORY) As ENROL_CATEGORY,
    FIAB.UMPT_REGU As FEEYEAR,
    Max(FIAB.AMOUNT) As AMOUNT,
    Count(FIAB.ACAD_PROG_FEE_TYPE) As COUNT,
    FIAB.QUALIFICATION,
    FIAB.QUALIFICATION_NAME,
    Max(FIAB.START_DATE) As START_DATE
From
    X020aa_Fiabd007 FIAB
Group By
    FIAB.FQUALLEVELAPID,
    FIAB.CAMPUS,
    FIAB.FPRESENTATIONCATEGORYCODEID,
    FIAB.FENROLMENTCATEGORYCODEID,
    FIAB.UMPT_REGU