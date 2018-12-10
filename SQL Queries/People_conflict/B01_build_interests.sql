﻿SELECT
  XXNWU_COI_INTERESTS.INTEREST_ID,
  XXNWU_COI_INTERESTS.DECLARATION_ID,
  XXNWU_COI_DECLARATIONS.EMPLOYEE_NUMBER,
  XXNWU_COI_DECLARATIONS.DECLARATION_DATE,
  XXNWU_COI_INTERESTS.CONFLICT_TYPE_ID,
  HR_LOOKUPS2.MEANING AS CONFLICT_TYPE,
  XXNWU_COI_INTERESTS.INTEREST_TYPE_ID,
  HR_LOOKUPS1.MEANING AS INTEREST_TYPE,
  XXNWU_COI_INTERESTS.STATUS_ID,
  HR_LOOKUPS3.MEANING AS INTEREST_STATUS,
  XXNWU_COI_INTERESTS.PERC_SHARE_INTEREST,
  XXNWU_COI_INTERESTS.ENTITY_NAME,
  XXNWU_COI_INTERESTS.ENTITY_REGISTRATION_NUMBER,
  XXNWU_COI_INTERESTS.OFFICE_ADDRESS,
  XXNWU_COI_INTERESTS.DESCRIPTION,
  XXNWU_COI_INTERESTS.DIR_APPOINTMENT_DATE,
  XXNWU_COI_INTERESTS.LINE_MANAGER,
  XXNWU_COI_INTERESTS.NEXT_LINE_MANAGER,
  XXNWU_COI_INTERESTS.INDUSTRY_CLASS_ID,
  HR_LOOKUPS.MEANING AS INDUSTRY_TYPE,
  XXNWU_COI_INTERESTS.TASK_PERF_AGREEMENT,
  XXNWU_COI_INTERESTS.MITIGATION_AGREEMENT,
  XXNWU_COI_INTERESTS.REJECTION_REASON,
  XXNWU_COI_INTERESTS.CREATION_DATE,
  XXNWU_COI_INTERESTS.AUDIT_USER,
  XXNWU_COI_INTERESTS.LAST_UPDATE_DATE,
  XXNWU_COI_INTERESTS.LAST_UPDATED_BY,
  XXNWU_COI_INTERESTS.EXTERNAL_REFERENCE
FROM
  XXNWU_COI_INTERESTS
  LEFT JOIN XXNWU_COI_DECLARATIONS ON XXNWU_COI_DECLARATIONS.DECLARATION_ID = XXNWU_COI_INTERESTS.DECLARATION_ID
  LEFT JOIN HR_LOOKUPS ON HR_LOOKUPS.LOOKUP_CODE = XXNWU_COI_INTERESTS.INTEREST_TYPE_ID AND HR_LOOKUPS.LOOKUP_TYPE =
    "NWU_COI_INDUSTRY_CLASS"
  LEFT JOIN HR_LOOKUPS HR_LOOKUPS1 ON HR_LOOKUPS1.LOOKUP_CODE = XXNWU_COI_INTERESTS.INTEREST_TYPE_ID AND
    HR_LOOKUPS1.LOOKUP_TYPE = "NWU_COI_INTEREST_TYPES"
  LEFT JOIN HR_LOOKUPS HR_LOOKUPS2 ON HR_LOOKUPS2.LOOKUP_CODE = XXNWU_COI_INTERESTS.CONFLICT_TYPE_ID AND
    HR_LOOKUPS2.LOOKUP_TYPE = "NWU_COI_CONFLICT_TYPE"
  LEFT JOIN HR_LOOKUPS HR_LOOKUPS3 ON HR_LOOKUPS3.LOOKUP_CODE = XXNWU_COI_INTERESTS.STATUS_ID AND
    HR_LOOKUPS3.LOOKUP_TYPE = "NWU_COI_STATUS"
