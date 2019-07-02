"""
Script to RUN GL TRANSACTION tests
Created: 2 Jul 2019
Author: Albert J van Rensburg (NWU21162395)
"""

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsys
import C202_gl_test_transactions

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C202_GL_TEST_TRANSACTIONS_RUN")
funcfile.writelog("-------------------------------------")

try:
    C202_gl_test_transactions.gl_test_transactions()
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C202_GL_TEST_TRANSACTIONS_RUN")
funcfile.writelog("----------------------------------------")

