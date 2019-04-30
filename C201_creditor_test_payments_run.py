
import sys

# OWN MODULE PATH
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

# IMPORT MODULES
import funcdate
import funcfile
import funcsys

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_PAYMENTS_RUN")
funcfile.writelog("---------------------------------------")

import C201_creditor_test_payments
try:
    C201_creditor_test_payments.Creditor_test_payments()
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_PAYMENTS_RUN")
funcfile.writelog("------------------------------------------")

