Select
    X000_Documents.DOC_HDR_ID,
    Count(X000_Documents.DOC_TYP_ID) As Count_DOC_TYP_ID
From
    X000_Documents
Group By
    X000_Documents.DOC_HDR_ID
