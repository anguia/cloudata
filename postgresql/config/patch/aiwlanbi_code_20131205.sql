DROP EXTERNAL TABLE ext_cboss_do_log;
CREATE EXTERNAL TABLE ext_cboss_do_log
(
  part1 text,
  part2 text,
  part3 text,
  part4 text,
  part5 text,
  part6 text,
  part7 text,
  part8 text,
  part9 text,
  part10 text,
  part11 text,
  part12 text,
  part13 text,
  part14 text,
  part15 text,
  part16 text,
  part17 text,
  part18 text,
  part19 text,
  part20 text
)
 LOCATION (
    'gpfdist://10.3.3.138:8004/domonitor*'
)
 FORMAT 'text' (delimiter '{' null E'\\N' escape E'\\' fill missing fields)
ENCODING 'GBK'
LOG ERRORS INTO err_ext_cboss_do_log SEGMENT REJECT LIMIT 200000000 ROWS;