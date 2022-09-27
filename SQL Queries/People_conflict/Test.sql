Select
    X000_interests_all.INTEREST_TYPE,
    Count(X000_interests_all.INTEREST_ID) As Count_INTEREST_ID
From
    X000_interests_all
Group By
    X000_interests_all.INTEREST_TYPE