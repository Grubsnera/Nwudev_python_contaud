Select
  *
From
  X003_dashboard_curr
Where
  (((X003_dashboard_curr.EMPLOYEE_NUMBER Is Not Null) And (X003_dashboard_curr.DECLARED Like 'NO%') And (X003_dashboard_curr.SUPERVISOR Is Not Null) And (X003_dashboard_curr.PERSON_TYPE Like 'EX%')) Or
  ((X003_dashboard_curr."EMPLOYEE_NUMBER:1" Is Not Null) And (X003_dashboard_curr.DECLARED Like 'NO%') And (X003_dashboard_curr.SUPERVISOR Is Not Null) And (X003_dashboard_curr.CATEGORY Like 'TEM%')) Or
  ((X003_dashboard_curr.DECLARED Like 'NO%') And (X003_dashboard_curr.CATEGORY Like 'PER%')))