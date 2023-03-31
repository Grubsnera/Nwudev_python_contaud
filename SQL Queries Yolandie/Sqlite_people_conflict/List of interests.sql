Select
    X001_declarations_curr.DECLARATION_ID,
    X001_declarations_curr.EMPLOYEE,
    X001_declarations_curr.EMP_SURNAME,
    X002_interests_curr.INTEREST_ID,
    X002_interests_curr.EMP_SURNAME As EMP_SURNAME1,
    X002_interests_curr.ENTITY_NAME
From
    X001_declarations_curr Inner Join
    X002_interests_curr On X002_interests_curr.DECLARATION_ID = X001_declarations_curr.DECLARATION_ID