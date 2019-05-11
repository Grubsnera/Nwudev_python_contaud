
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
funcfile.writelog("SCRIPT: C301_REPORT_STUDENT_DEFERMENT_RUN")
funcfile.writelog("-----------------------------------------")

import C301_report_student_deferment
try:
    C301_report_student_deferment.Studdeb_deferments('curr',funcdate.cur_year())
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C301_REPORT_STUDENT_DEFERMENT_RUN")
funcfile.writelog("--------------------------------------------")

