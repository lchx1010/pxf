-- @description query01 for PXF HDFS Writable Avro with user-provided schema on Classpath, complex types with arrays

-- start_matchsubs
--
-- # create a match/subs
--
-- end_matchsubs
SELECT * from writable_avro_complex_user_schema_w_array_on_classpath_readable ORDER BY type_int;
