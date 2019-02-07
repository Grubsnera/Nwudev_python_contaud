
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS_RUN")
funcfile.writelog("--------------------____------")

import B004_payroll_lists

# Environment
l_mail = True

# Vss lists
try:
    B004_payroll_lists.Payroll_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: B004_PAYROLL_LISTS_RUN")
funcfile.writelog("---------------------------------")
 

