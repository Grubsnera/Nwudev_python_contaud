Select
    x003d.DECLARED,
    x003d.PERSON_TYPE,
    Count(x003d.EMPLOYEE) As Count_EMPLOYEE
From
    X003_dashboard_curr x003d
Group By
    x003d.DECLARED,
    x003d.PERSON_TYPE