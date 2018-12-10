
# Add own module path
import sys
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

# Import the system objects
import datetime
import calendar
import time

# Import own modules
import funcdate

# FUNCTION TO WRITE INTO LOG FILE **********************************************



s_entry = "%t TESTING"
s_path = "S:/Logs/"
s_file = "Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt"

# Open the log file

try:
    with open(s_path + s_file, 'a') as fl:
        # file opened for writing. write to it here
        # Write the log
        if s_entry == "Now":
            fl.write("----------------\r\n")
            s_entry = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+"\r\n"
        elif "%t" in s_entry:
            s_entry = s_entry.replace("%t",datetime.datetime.now().strftime("%H:%M:%S"))
            s_entry += "\r\n"
        else:
            s_entry += "\r\n"
        fl.write(s_entry)
        fl.close()
        print("Writed")
        pass
except IOError as x:
    print('error ',x.errno,',', x.strerror)
    """
    if x.errno == errno.EACCES:
        print( s_file, 'no perms')
    elif x.errno == errno.EISDIR:
        print( s_file, 'is directory')
    """
