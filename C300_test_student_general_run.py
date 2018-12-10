
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')


import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL_RUN")
funcfile.writelog("-------------------------------------")

import C300_test_student_general

# Environment
l_mail = True

# Vss general tests
try:
    C300_test_student_general.Test_student_general()
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C300_TEST_STUDENT_GENERAL_RUN")
funcfile.writelog("----------------------------------------")

