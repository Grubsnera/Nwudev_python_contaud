SELECT
  *
FROM
  X000_GL_trans_curr
WHERE
  (X000_GL_trans_curr.FIN_OBJECT_CD = '7551') OR
  (X000_GL_trans_curr.FIN_OBJECT_CD = '7552') OR
  (X000_GL_trans_curr.FIN_OBJECT_CD = '7553')
