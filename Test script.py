# TEST GROUND FOR SCRIPTS
# CAN BE DELETED - NO PERMANENT CODE STORED HERE

# SYSTEM MODULES

# IMPORT OWN MODULES

# GET PREVIOUS FINDINGS
if i_find > 0:
    i = functest.get_previous_finding(so_curs, ed_path, "?.txt", "?", "ITTTT")
    print(i)
    so_conn.commit()

# SET PREVIOUS FINDINGS
if i_find > 0:
    i = functest.set_previous_finding(so_curs)
    print(i)
    so_conn.commit()

# IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
if i_find > 0 and i_coun > 0:
    i = functest.get_officer(so_curs, "?")
    print(i)
    so_conn.commit()

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
if i_find > 0 and i_coun > 0:
    i = functest.get_supervisor(so_curs, "?")
    print(i)
    so_conn.commit()
