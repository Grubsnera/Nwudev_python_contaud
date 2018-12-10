SELECT
  A01_all_declerations.DECLARATION_ID,
  A01_all_declerations.EMPLOYEE_NUMBER,
  A01_all_declerations.DECLARATION_DATE,
  A01_all_declerations.UNDERSTAND_POLICY_FLAG,
  A01_all_declerations.INTEREST_TO_DECLARE_FLAG,
  A01_all_declerations.FULL_DISCLOSURE_FLAG,
  A01_all_declerations.STATUS,
  A01_all_declerations.LINE_MANAGER,
  A01_all_declerations.REJECTION_REASON,
  A01_all_declerations.CREATION_DATE,
  A01_all_declerations.AUDIT_USER,
  A01_all_declerations.LAST_UPDATE_DATE,
  A01_all_declerations.LAST_UPDATED_BY,
  A01_all_declerations.EXTERNAL_REFERENCE
FROM
  A01_all_declerations
WHERE
  A01_all_declerations.DECLARATION_DATE >= Date("2018-01-01") AND
  A01_all_declerations.DECLARATION_DATE <= Date("2018-12-31")
