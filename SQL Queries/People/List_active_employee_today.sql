Select
    pap.EMPLOYEE_NUMBER As employee_number,
    pap.PERSON_ID As person_id,
    paa.ASSIGNMENT_ID As assignment_id,
    pap.FULL_NAME As name_full,
    upper(hrl.LOCATION_CODE) As location,
    Max(pps.ACTUAL_TERMINATION_DATE) As service_end_date,
    upper(ppt.USER_PERSON_TYPE) As user_person_type,
    upper(cat.MEANING) As assignment_category,
    Case
        When paa.position_id = 0 Then upper(paa.EMPLOYEE_CATEGORY)
        Else upper(hrp.acad_supp)
    End as employee_category,
    upper(hrp.position_name) position_name
From
    PER_ALL_PEOPLE_F pap Left Join
    PER_ALL_ASSIGNMENTS_F paa On paa.PERSON_ID = pap.PERSON_ID
            And Date() Between paa.EFFECTIVE_START_DATE And paa.EFFECTIVE_END_DATE
            And paa.EFFECTIVE_END_DATE Between pap.EFFECTIVE_START_DATE And pap.EFFECTIVE_END_DATE
            And paa.ASSIGNMENT_STATUS_TYPE_ID = 1 Left Join
    PER_PERIODS_OF_SERVICE pps On pps.PERSON_ID = pap.PERSON_ID Left Join
    PER_PERSON_TYPE_USAGES_F ptu On pap.PERSON_ID = ptu.PERSON_ID
            And paa.EFFECTIVE_END_DATE Between ptu.EFFECTIVE_START_DATE And ptu.EFFECTIVE_END_DATE Left Join
    PER_PERSON_TYPES ppt On ptu.PERSON_TYPE_ID = ppt.PERSON_TYPE_ID Left Join
    HR_LOCATIONS_ALL hrl On hrl.LOCATION_ID = paa.LOCATION_ID Left Join
    HR_LOOKUPS cat On cat.LOOKUP_CODE = paa.EMPLOYMENT_CATEGORY
            And cat.LOOKUP_TYPE = 'EMP_CAT' Left Join
    X000_POSITIONS hrp On hrp.POSITION_ID = paa.POSITION_ID
            And paa.EFFECTIVE_END_DATE Between hrp.EFFECTIVE_START_DATE And hrp.EFFECTIVE_END_DATE    
Where
    (paa.EFFECTIVE_END_DATE Between pps.DATE_START And pps.ACTUAL_TERMINATION_DATE And
        ppt.USER_PERSON_TYPE != 'Retiree') Or
    (ppt.USER_PERSON_TYPE != 'Retiree' And
        pps.ACTUAL_TERMINATION_DATE == pps.DATE_START)
Group By
    pap.EMPLOYEE_NUMBER