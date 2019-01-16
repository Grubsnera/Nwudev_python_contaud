SELECT
  X000_Students_curr.*,
  X000_Tran_balopen_curr.BAL_OPEN
FROM
  X000_Students_curr
  LEFT JOIN X000_Tran_balopen_curr ON X000_Tran_balopen_curr.STUDENT = X000_Students_curr.KSTUDBUSENTID
