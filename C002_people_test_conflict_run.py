"""
Script to run C002_people_test_conflict
"""

# IMPORT MODULES
from _my_modules import funcfile
from _my_modules import funcsys

import C002_people_test_conflict

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_RUN")
funcfile.writelog("-------------------------------------")

try:
    C002_people_test_conflict.People_test_conflict()
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_RUN")
funcfile.writelog("----------------------------------------")
