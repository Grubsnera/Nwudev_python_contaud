﻿Select
    PARTY.PARTYTYPE,
    Count(PARTY.KBUSINESSENTITYID) As Count_KBUSINESSENTITYID
From
    PARTY
Group By
    PARTY.PARTYTYPE