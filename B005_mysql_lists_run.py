

from _my_modules import funcfile
from _my_modules import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B005_RUN_MYSQL")
funcfile.writelog("----------------------")

import B005_mysql_lists

# Environment
l_mail = True

# People lists
try:
    B005_mysql_lists.mysql_lists("Web_ia_nwu")
    # B005_mysql_lists.mysql_lists("Mysql_ia_server")
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: B005_RUN_MYSQL")
funcfile.writelog("-------------------------")
