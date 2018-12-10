﻿SELECT
  X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
  X002_PEOPLE_CURR.PERSON_ID,
  X002_PEOPLE_CURR.FULL_NAME,
  X002_PEOPLE_CURR.KNOWN_NAME,
  X002_PEOPLE_CURR.DATE_EMP_LOOKUP,
  X000_ADDRESS_SARS.ADDRESS_SARS,
  X000_ADDRESS_POST.ADDRESS_POST,
  X000_ADDRESS_HOME.ADDRESS_HOME,
  X000_ADDRESS_OTHE.ADDRESS_OTHE
FROM
  X002_PEOPLE_CURR
  LEFT JOIN X000_ADDRESS_SARS ON X000_ADDRESS_SARS.PERSON_ID = X002_PEOPLE_CURR.PERSON_ID AND
    X000_ADDRESS_SARS.DATE_FROM <= X002_PEOPLE_CURR.DATE_EMP_LOOKUP AND X000_ADDRESS_SARS.DATE_TO >=
    X002_PEOPLE_CURR.DATE_EMP_LOOKUP
  LEFT JOIN X000_ADDRESS_POST ON X000_ADDRESS_POST.PERSON_ID = X002_PEOPLE_CURR.PERSON_ID AND
    X000_ADDRESS_POST.DATE_FROM <= X002_PEOPLE_CURR.DATE_EMP_LOOKUP AND X000_ADDRESS_POST.DATE_TO >=
    X002_PEOPLE_CURR.DATE_EMP_LOOKUP
  LEFT JOIN X000_ADDRESS_HOME ON X000_ADDRESS_HOME.PERSON_ID = X002_PEOPLE_CURR.PERSON_ID AND
    X000_ADDRESS_HOME.DATE_FROM <= X002_PEOPLE_CURR.DATE_EMP_LOOKUP AND X000_ADDRESS_HOME.DATE_TO >=
    X002_PEOPLE_CURR.DATE_EMP_LOOKUP
  LEFT JOIN X000_ADDRESS_OTHE ON X000_ADDRESS_OTHE.PERSON_ID = X002_PEOPLE_CURR.PERSON_ID AND
    X000_ADDRESS_OTHE.DATE_FROM <= X002_PEOPLE_CURR.DATE_EMP_LOOKUP AND
    X000_ADDRESS_OTHE.DATE_TO >= X002_PEOPLE_CURR.DATE_EMP_LOOKUP
