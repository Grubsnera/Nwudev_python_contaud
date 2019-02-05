SELECT
  X001aa_Students.*,
  X000_Deferments_curr.*
FROM
  X001aa_Students
  LEFT JOIN X000_Deferments_curr ON X000_Deferments_curr.STUDENT = X001aa_Students.KSTUDBUSENTID
WHERE
  X001aa_Students.BAL_REG_CALC > 1000
