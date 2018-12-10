SELECT
  X001ab_gl_transort.COST_STRING,
  X001ab_gl_transort.TIME_POST,
  X001ab_gl_transort.EDOC,
  X001ab_gl_transort.DESC_FULL,
  X001ab_gl_transort.AMOUNT,
  Trim(X001ab_gl_transort.COST_STRING) || X001ab_gl_transort.TIME_POST AS "ROWID"
FROM
  X001ab_gl_transort
WHERE
  X001ab_gl_transort.CAMPUS IS NULL
