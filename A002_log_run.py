"""
Script to run log capture
Created on: 9 Sep 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT SYSTEM OBJECTS

# IMPORT OWN OBJECTS
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsys
import A002_log

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A002_LOG_RUN")
funcfile.writelog("--------------------")

try:
    # A002_log.log_capture("2019-09-10", False)
    # A002_log.log_capture(funcdate.yesterday(), False)
    A002_log.log_capture(funcdate.today(), False)
except Exception as e:
    funcsys.ErrMessage(e)

funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A002_LOG_RUN")
funcfile.writelog("-----------------------")
