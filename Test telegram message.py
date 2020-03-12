# TEST GROUND FOR SCRIPTS
# CAN BE DELETED - NO PERMANENT CODE STORED HERE

# SYSTEM MODULES
import csv

# IMPORT OWN MODULES
from _my_modules import funcsms

# SEND FIRST MESSAGE
print(funcsms.send_telegram("Dear", "administrator", 'Test message from <b>Telegram @nwuia_bot</b> robot.'))
