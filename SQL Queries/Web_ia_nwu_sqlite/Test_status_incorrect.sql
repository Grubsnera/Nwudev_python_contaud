Select
    assc.File,
    assc.Auditor,
    assc.Year,
    assc.Category,
    assc.Type,
    assc.Priority_word As Priority,
    assc.Assignment_status_calc As Status,
    assc.Date_closed_calc As Date_closed,
    assc.Assignment,
    assc.ia_user_mail,
    assc.Email_manager1,
    assc.Email_manager2
From
    X000_Assignment_curr assc
Where
    assc.Priority_word Like ('9%') And
    assc.Assignment_status_calc Not Like ('9%')