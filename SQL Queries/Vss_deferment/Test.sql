﻿SELECT
  X001aa_Students.KSTUDBUSENTID,
  Count(X001aa_Students.KENROLSTUDID) AS Count_KENROLSTUDID
FROM
  X001aa_Students
GROUP BY
  X001aa_Students.KSTUDBUSENTID