# Import python objects


# Add own module path
sys.path.append('X:\\Python\\_my_modules')
#print(sys.path)

# Import own modules
import funcfile

# Open the script log file
funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("DEFINITION: Read email list")
ilog_severity = 1

# Import own functions
import funcdate # Date functions
import funcmail # Mailing functions

# Declare the global variables ************************************************
sl_path = "X:/Python/"

print("SENDING EMAIL")
print("-------------")

# Read the mail parameters from the 000_Mail.csv file """
co = open(sl_path + "000m_mail.csv", "rU")
co_reader = csv.reader(co)

# Read the COLUMN database data

for row in co_reader:

    # Populate the local variables
    s_result = ""
    s_trigger = ""
    to_name = ""
    to_address = ""
    mail_language = ""
    mail_subject = ""
    mail_body = ""
    file_path = ""
    file_name = ""

    # Populate the column variables
    if row[0] != "test":
        continue
    
    elif row[1] == "X":
        continue
    
    else:

        s_trigger = row[0]
        to_name = row[2]
        to_address = row[3]
        mail_language = row[4]
        mail_subject = row[5]
        mail_body = row[6]
        file_path = row[7]
        file_path = file_path.replace("%PYEAR%",funcdate.prev_year())
        file_path = file_path.replace("%PMONTH%",funcdate.prev_month())
        file_path = file_path.replace("%CYEAR%",funcdate.cur_year())
        file_path = file_path.replace("%CMONTH%",funcdate.cur_month())
        file_name = row[8]
        file_name = file_name.replace("%PYEAR%",funcdate.prev_year())
        file_name = file_name.replace("%PMONTH%",funcdate.prev_month())
        file_name = file_name.replace("%CYEAR%",funcdate.cur_year())
        file_name = file_name.replace("%CMONTH%",funcdate.cur_month())

        s_result = funcmail.send(to_name,to_address,mail_language,mail_subject,mail_body,file_path,file_name)

        # Mail result log
        if s_result == "Successfully sent email":
            print("MAIL SUCCESS: " + to_address + "(" + to_name + ")")
            funcfile.writelog("MAIL SUCCESS: " + s_trigger + "_" + to_address + "(" + to_name + ")")
        else:
            print("MAIL FAIL: " + to_address + "(" + to_name + ")")
            print("FAIL REASON: " + s_result)
            funcfile.writelog("MAIL FAIL: " + s_trigger + "_" + to_address + "(" + to_name + ")")
            funcfile.writelog("REASON FAIL: " + s_result)
        
# Close the impoted data file
co.close()

# Close the log writer *********************************************************
funcfile.writelog("COMPLETED")
