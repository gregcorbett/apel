-- This script contains multiple comment blocks that can update
-- APEL version 1.6.1 databases of the following types to the next version:
--  - Cloud Accounting Database

-- To update, find the relevent comment block below and remove
-- its block comment symbols /* and */ then run this script.

-- UPDATE SCRIPT FOR CLOUD SCHEMA

-- If you have a Cloud Accounting Database and wish to
-- upgrade to APEL Version next, remove the block comment
-- symbols around this section and run this script

-- This section will:
-- - Change records with a NULL CpuCount so that they have a CpuCount of 1,
--   to prevent summariser time problems.
-- - Change records with a NULL StartTime so that
--   they have a StartTime of '0000-00-00 00:00:00'
--   to prevent summariser time problems.


UPDATE CloudRecords SET
    CpuCount=1
    WHERE CpuCount is NULL;

UPDATE CloudRecords SET
    StartTime='0000-00-00 00:00:00' 
    WHERE StartTime is NULL;
    
