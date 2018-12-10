SELECT
  B01_all_interests.INTEREST_ID,
  B01_all_interests.DECLARATION_ID,
  B01_all_interests.EMPLOYEE_NUMBER,
  B01_all_interests.DECLARATION_DATE,
  B01_all_interests.CONFLICT_TYPE,
  B01_all_interests.INTEREST_TYPE,
  B01_all_interests.PERC_SHARE_INTEREST,
  B01_all_interests.INDUSTRY_TYPE,
  B01_all_interests.ENTITY_NAME,
  B01_all_interests.ENTITY_REGISTRATION_NUMBER,
  B01_all_interests.OFFICE_ADDRESS,
  B01_all_interests.DESCRIPTION,
  B01_all_interests.INTEREST_STATUS,
  B01_all_interests.LINE_MANAGER,
  B01_all_interests.NEXT_LINE_MANAGER,
  B01_all_interests.DIR_APPOINTMENT_DATE,
  B01_all_interests.TASK_PERF_AGREEMENT,
  B01_all_interests.MITIGATION_AGREEMENT,
  B01_all_interests.REJECTION_REASON,
  B01_all_interests.CREATION_DATE,
  B01_all_interests.AUDIT_USER,
  B01_all_interests.LAST_UPDATE_DATE,
  B01_all_interests.LAST_UPDATED_BY,
  B01_all_interests.EXTERNAL_REFERENCE
FROM
  A02_declarations2018
  LEFT JOIN B01_all_interests ON B01_all_interests.DECLARATION_ID = A02_declarations2018.DECLARATION_ID
WHERE
  A02_declarations2018.INTEREST_TO_DECLARE_FLAG == "Y"
ORDER BY
  B01_all_interests.EMPLOYEE_NUMBER,
  B01_all_interests.LAST_UPDATE_DATE
