﻿SELECT
  a.STUDENT_VSS,
  a.TRANSDATE_VSS,
  a.AMOUNT_VSS,
  Count(a.STUDENT_VSS) AS COUNT
FROM
  a
GROUP BY
  a.STUDENT_VSS,
  a.TRANSDATE_VSS,
  a.AMOUNT_VSS
