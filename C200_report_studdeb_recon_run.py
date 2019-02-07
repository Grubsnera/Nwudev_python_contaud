
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_RUN")
funcfile.writelog("-------------------------------------")

import C200_report_studdeb_recon

# Environment
l_mail = True

# Kfs reports
try:
    C200_report_studdeb_recon.Report_studdeb_recon('66541143.01','-18450460.22','41049069.28')
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_RUN")
funcfile.writelog("----------------------------------------")

