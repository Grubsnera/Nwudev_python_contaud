Select
    ph.PARENT_ID,
    ph.PHONE_NUMBER,
    Max(ph.DATE_FROM) As DATE_FROM,
    ph.DATE_TO,
    ph.PHONE_ID
From
    PER_PHONES ph
Where
    ph.PHONE_TYPE = 'M' And
    ph.PARENT_TABLE = 'PER_ALL_PEOPLE_F'
Group By
    ph.PARENT_ID