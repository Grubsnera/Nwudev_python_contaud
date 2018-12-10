SELECT
  X001cx_Stud_qual_curr.*
FROM
  X001cx_Stud_qual_curr
WHERE
  (X001cx_Stud_qual_curr.DATEENROL <= Date('2018-06-30') AND
  X001cx_Stud_qual_curr.DISCONTINUEDATE >= Date('2018-06-30') AND
  Upper(X001cx_Stud_qual_curr.QUAL_TYPE) != 'SHORT COURSE') OR
  (X001cx_Stud_qual_curr.DATEENROL <= Date('2018-06-30') AND
  X001cx_Stud_qual_curr.DISCONTINUEDATE IS NULL AND
  Upper(X001cx_Stud_qual_curr.QUAL_TYPE) != 'SHORT COURSE')
