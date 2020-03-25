﻿Select
    NEW.VENDOR_CATEGORY,
    NEW.VENDOR_ID,
    NEW.VENDOR_MAIL,
    CNG.VENDOR_CATEGORY As VENDOR_CATEGORY1,
    CNG.VENDOR_ID As VENDOR_ID1,
    CNG.VENDOR_MAIL_OLD,
    CNG.VENDOR_MAIL_NEW
From
    X100baac_employee_vendor_share_email NEW Left Join
    X100bab_employee_vendor_share_email CNG On CNG.VENDOR_ID = NEW.VENDOR_ID