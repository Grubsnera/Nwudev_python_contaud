﻿SELECT
  PAY_RUN_BALANCES_CURR.*,
  PAY_DEFINED_BALANCES.*,
  PAY_BALANCE_TYPES.*
FROM
  PAY_RUN_BALANCES_CURR
  LEFT JOIN PAY_DEFINED_BALANCES ON PAY_DEFINED_BALANCES.DEFINED_BALANCE_ID = PAY_RUN_BALANCES_CURR.DEFINED_BALANCE_ID
  LEFT JOIN PAY_BALANCE_TYPES ON PAY_BALANCE_TYPES.BALANCE_TYPE_ID = PAY_DEFINED_BALANCES.BALANCE_TYPE_ID
