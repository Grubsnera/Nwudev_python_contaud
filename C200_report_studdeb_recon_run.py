"""
Script to run C200_REPORT_STUDDEB_RECON_RUN
"""

# Import own modules
from _my_modules import funcfile
from _my_modules import funcsys

import C200_report_studdeb_recon

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_RUN")
funcfile.writelog("-------------------------------------")

# Environment
l_mail = True

# Kfs reports
try:
    C200_report_studdeb_recon.Report_studdeb_recon()
    # 2019 Opening balances
    # C200_report_studdeb_recon.Report_studdeb_recon('66561452.48','-18340951.06','39482933.18')
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_RUN")
funcfile.writelog("----------------------------------------")
