"""
Functions to handle file functions
Author: AB Janse v Rensburg
Create: 24 Jan 2018
"""

# IMPORT SYSTEM OBJECTS
import datetime
import time

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys


def writelog(s_entry="\n", s_path="S:/Logs/",
             s_file="Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt"):
    """
    Function to create log file
    :param s_entry: Log file entry
    :param s_path: Log file path
    :param s_file: Log file name
    :return: Nothing
    """

    # DECLARE VARIABLES
    l_success: bool = False
    s_file = "Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt"
    s_project: str = "FUNCFILE:" + s_file

    try:

        with open(s_path + s_file, 'a', encoding="utf-8") as fl:
            # file opened for writing. write to it here
            # Write the log
            if s_entry == "Now":
                fl.write("----------------\n")
                s_entry = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+"\n"
            elif "%t" in s_entry:
                s_entry = s_entry.replace("%t", datetime.datetime.now().strftime("%H:%M:%S"))
                s_entry += "\n"
            else:
                s_entry += "\n"
            fl.write(s_entry)
            fl.close()
            pass
            l_success = True

    except Exception as err:

        l_success = False
        funcsys.ErrMessage(err, funcconf.l_mail_project,
                           "NWUIACA:Fail:" + s_project,
                           "NWUIACA: Fail: " + s_project)

    return l_success
