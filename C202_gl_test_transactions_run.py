"""
Script to RUN the GL TRANSACTION TESTS
Created: 2 Jul 2019
Author: Albert B Janse van Rensburg
"""

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsys
import C202_gl_test_transactions

# DECLARE VARIABLES
l_mail = False

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C202_GL_TEST_TRANSACTIONS_RUN")
funcfile.writelog("-------------------------------------")

try:
    C202_gl_test_transactions.gl_test_transactions()
    if l_mail:
        funcmail.Mail('std_success_gmail', 'NWUIAPython:Success:C202_GL_TEST_TRANSACTIONS', 'NWUIAPython: Success: C202_GL_TEST_TRANSACTIONS')
except Exception as e:
    funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:C202_GL_TEST_TRANSACTIONS', 'NWUIAPython: Fail: C202_GL_TEST_TRANSACTIONS')

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C202_GL_TEST_TRANSACTIONS_RUN")
funcfile.writelog("----------------------------------------")
