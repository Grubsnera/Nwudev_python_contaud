Select
    x000p.may_be_nominated,
    x000p.may_vote,
    Count(x000p.nominated) As Count_nominated
From
    X000_PEOPLE_LIST_SELECTED x000p
Group By
    x000p.may_be_nominated,
    x000p.may_vote