DROP TABLE temp_apache_log_add_day;

CREATE TABLE temp_apache_log_add_day
(
  odate date, -- 日期（去到天）
  prov_id integer, -- 省份标识
  page_type integer, -- 页面类型，编码参照维表
  user_ip inet, -- 用户IP
  p_count numeric
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (user_ip)
PARTITION BY RANGE(odate) 
          (
          PARTITION prt_20130422 START ('2013-04-22'::date) END ('2013-04-23'::date) WITH (orientation=row, appendonly=true, compresstype=zlib, compresslevel=5), 
          PARTITION prt_20130618 START ('2013-06-18'::date) END ('2013-06-19'::date) WITH (appendonly=true, compresslevel=5, compresstype=zlib)
          )
;

COMMENT ON TABLE temp_apache_log_add_day
  IS '与Apache log(src_apache_log)每天比较的叠加数据。分布键USER_IP';
COMMENT ON COLUMN temp_apache_log_add_day.odate IS '日期（去到天）';
COMMENT ON COLUMN temp_apache_log_add_day.prov_id IS '省份标识';
COMMENT ON COLUMN temp_apache_log_add_day.page_type IS '页面类型，编码参照维表';
COMMENT ON COLUMN temp_apache_log_add_day.user_ip IS '用户IP';
