﻿SELECT
  FINAIDSITE.KFINAIDSITEID,
  FINAIDSITE.FFINAIDID,
  FINAIDSITE.FSITEORGUNITNUMBER,
  X004aa_Bursaries.FFINAIDINSTBUSENTID,
  X004aa_Bursaries.FINAIDCODE,
  X004aa_Bursaries.FINAIDNAME,
  X004aa_Bursaries.FINAIDNAAM,
  X004aa_Bursaries.FTYPECODEID,
  X004aa_Bursaries.TYPE_E,
  X004aa_Bursaries.TYPE_A,
  X004aa_Bursaries.FFINAIDCATCODEID,
  X004aa_Bursaries.BURS_CATE_E,
  X004aa_Bursaries.BURS_CATE_A,
  X004aa_Bursaries.ISAUTOAPPL,
  X004aa_Bursaries.ISWWWAPPLALLOWED,
  X004aa_Bursaries.FINAIDYEARS,
  X004aa_Bursaries.FFUNDTYPECODEID,
  X004aa_Bursaries.FUND_TYPE_E,
  X004aa_Bursaries.FUND_TYPE_A,
  X004aa_Bursaries.FSTUDYTYPECODEID,
  X004aa_Bursaries.STUDY_TYPE_E,
  X004aa_Bursaries.STUDY_TYPE_A,
  FINAIDSITE.CC,
  FINAIDSITE.ACC,
  FINAIDSITE.LOANTYPECODE,
  FINAIDSITE.STARTDATE,
  FINAIDSITE.ENDDATE,
  FINAIDSITE.FCOAID,
  FINAIDSITE.AUDITDATETIME,
  FINAIDSITE.FAUDITSYSTEMFUNCTIONID,
  FINAIDSITE.FAUDITUSERCODE
FROM
  FINAIDSITE
  LEFT JOIN X004aa_Bursaries ON X004aa_Bursaries.KFINAIDID = FINAIDSITE.FFINAIDID
