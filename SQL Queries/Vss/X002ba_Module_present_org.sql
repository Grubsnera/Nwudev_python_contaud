﻿SELECT
  MODULEPRESENTINGOU.KPRESENTINGOUID,
  MODULEPRESENTINGOU.STARTDATE,
  MODULEPRESENTINGOU.ENDDATE,
  MODULEPRESENTINGOU.FBUSINESSENTITYID,
  X000_Orgunitinstance.FSITEORGUNITNUMBER,
  X000_Orgunitinstance.ORGUNIT_TYPE,
  X000_Orgunitinstance.ORGUNIT_NAME,
  MODULEPRESENTINGOU.FMODULEAPID,
  X002aa_Module.COURSECODE,
  X002aa_Module.COURSELEVEL,
  X002aa_Module.COURSEMODULE,
  MODULEPRESENTINGOU.FCOURSEGROUPCODEID,
  X000_Codedescription_coursegroup.LONG AS NAME_GROUP,
  X000_Codedescription_coursegroup.LANK AS NAAM_GROEP,
  MODULEPRESENTINGOU.ISEXAMMODULE,
  MODULEPRESENTINGOU.LOCKSTAMP,
  MODULEPRESENTINGOU.AUDITDATETIME,
  MODULEPRESENTINGOU.FAUDITSYSTEMFUNCTIONID,
  MODULEPRESENTINGOU.FAUDITUSERCODE
FROM
  MODULEPRESENTINGOU
  LEFT JOIN X000_Orgunitinstance ON X000_Orgunitinstance.KBUSINESSENTITYID = MODULEPRESENTINGOU.FBUSINESSENTITYID
  LEFT JOIN X002aa_Module ON X002aa_Module.MODULE_ID = MODULEPRESENTINGOU.FMODULEAPID
  LEFT JOIN X000_Codedescription X000_Codedescription_coursegroup ON X000_Codedescription_coursegroup.KCODEDESCID =
    MODULEPRESENTINGOU.FCOURSEGROUPCODEID
