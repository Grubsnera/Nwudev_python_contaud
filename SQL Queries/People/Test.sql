Select
    papf.PERSON_ID,
    Count(papf.BUSINESS_GROUP_ID) As Count_BUSINESS_GROUP_ID
From
    PER_ALL_PEOPLE_F papf
Group By
    papf.PERSON_ID