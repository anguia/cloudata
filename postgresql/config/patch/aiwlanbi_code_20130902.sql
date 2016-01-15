ALTER TABLE rpt_cha_nasip_day                          
   ALTER COLUMN err_reason TYPE character varying(100);
ALTER TABLE src_monitor_log                             
   ALTER COLUMN user_name TYPE character varying(64);   
ALTER TABLE src_monitor_log                             
   ALTER COLUMN op_type TYPE character varying(100);    
ALTER TABLE src_monitor_log                             
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE src_radius_log
   ALTER COLUMN user_name TYPE character varying(128);
ALTER TABLE src_subscription
   ALTER COLUMN bms_user_name TYPE character varying(64);
ALTER TABLE src_subscription
   ALTER COLUMN bms_user_password TYPE character varying(20);
ALTER TABLE src_usage
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE src_usage
   ALTER COLUMN nas_identifier TYPE character varying(64);
ALTER TABLE src_usage
   ALTER COLUMN user_domain TYPE character varying(40);
ALTER TABLE src_usage
   ALTER COLUMN mac_addr TYPE character varying(32);
DROP TABLE src_wlan_package;

CREATE TABLE src_wlan_package
(
  bms_user_name character varying(64), -- 用户账号
  package_code character varying(16), -- 套餐编号
  package_name character varying(64), -- 套餐名称
  abs_effect_time timestamp without time zone, -- 生效时间
  abs_expire_time timestamp without time zone, -- 失效时间
  bms_prefix_type integer, -- 套餐类型:1—时长套餐 2—流量套餐 3—流量套餐叠加包
  abs_res_open numeric(15,0), -- 套餐初始资源（时长单位秒,流量单位K字节）0表示无资源限制
  time_stamp timestamp without time zone -- 记录更新时间
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED randomly;
ALTER TABLE src_wlan_package
  OWNER TO aidns;
COMMENT ON TABLE src_wlan_package
  IS '叠加资源表';
COMMENT ON COLUMN src_wlan_package.bms_user_name IS '用户账号';
COMMENT ON COLUMN src_wlan_package.package_code IS '套餐编号';
COMMENT ON COLUMN src_wlan_package.package_name IS '套餐名称';
COMMENT ON COLUMN src_wlan_package.abs_effect_time IS '生效时间';
COMMENT ON COLUMN src_wlan_package.abs_expire_time IS '失效时间';
COMMENT ON COLUMN src_wlan_package.bms_prefix_type IS '套餐类型:1—时长套餐 2—流量套餐 3—流量套餐叠加包 ';
COMMENT ON COLUMN src_wlan_package.abs_res_open IS '套餐初始资源（时长单位秒,流量单位K字节）0表示无资源限制';
COMMENT ON COLUMN src_wlan_package.time_stamp IS '记录更新时间';

DROP TABLE src_wlan_user_cookie;

CREATE TABLE src_wlan_user_cookie
(
  bms_user_name character varying(64), -- 用户帐号
  abs_effect_time timestamp without time zone, -- Cookie认证生效时间
  abs_expire_time timestamp without time zone, -- Cookie认证失效时间
  abs_effect_days integer, -- Cookie有效天数
  ua_type integer, -- UA类型
  time_stamp timestamp without time zone -- 记录更新时间
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED randomly;
ALTER TABLE src_wlan_user_cookie
  OWNER TO aidns;
COMMENT ON TABLE src_wlan_user_cookie
  IS 'WLAN用户Cookie源表';
COMMENT ON COLUMN src_wlan_user_cookie.bms_user_name IS '用户帐号';
COMMENT ON COLUMN src_wlan_user_cookie.abs_effect_time IS 'Cookie认证生效时间';
COMMENT ON COLUMN src_wlan_user_cookie.abs_expire_time IS 'Cookie认证失效时间';
COMMENT ON COLUMN src_wlan_user_cookie.abs_effect_days IS 'Cookie有效天数';
COMMENT ON COLUMN src_wlan_user_cookie.ua_type IS 'UA类型';
COMMENT ON COLUMN src_wlan_user_cookie.time_stamp IS '记录更新时间';

DROP TABLE sys_tele_provider;

CREATE TABLE sys_tele_provider
(
  msisdn_header character varying(3),
  provider_id integer,
  provider_name character varying(10)
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (msisdn_header);

ALTER TABLE temp_account_attack
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_account_attack_1
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_account_attack_1
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_active_user_day
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_active_user_month
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_cha_err
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_fixed_param_attack
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_fixed_param_attack_1
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_fixed_param_attack_1
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_ip_attack
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_ip_attack_1
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_ip_attack_1
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_monitor_log_1
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_monitor_log_1
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_monitor_log_2
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_monitor_log_2
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_normal_login_request
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_normal_login_request
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_src_monitor_log
   ALTER COLUMN user_name TYPE character varying(64);
ALTER TABLE temp_src_monitor_log
   ALTER COLUMN op_type TYPE character varying(100);
ALTER TABLE temp_src_monitor_log
   ALTER COLUMN detail_info TYPE character varying(100);
ALTER TABLE temp_src_radius_log
   ALTER COLUMN user_name TYPE character varying(128);
   
DROP VIEW all_sys_prov_acip_info;

ALTER TABLE sys_prov_acip_info
   ALTER COLUMN ac_city_name TYPE character varying(20);
ALTER TABLE sys_prov_acip_info
   ALTER COLUMN ac_name TYPE character varying(50);

ALTER TABLE temp_sys_prov_acip_info
   ALTER COLUMN ac_city_name TYPE character varying(20);
ALTER TABLE temp_sys_prov_acip_info
   ALTER COLUMN ac_name TYPE character varying(50);


CREATE OR REPLACE VIEW all_sys_prov_acip_info AS 
 SELECT temp_sys_prov_acip_info.idx, temp_sys_prov_acip_info.ac_prov_id, temp_sys_prov_acip_info.ac_city_name, temp_sys_prov_acip_info.ac_name, temp_sys_prov_acip_info.ac_ip, temp_sys_prov_acip_info.create_time, temp_sys_prov_acip_info.update_time
   FROM temp_sys_prov_acip_info
UNION ALL 
 SELECT sys_prov_acip_info.idx, sys_prov_acip_info.ac_prov_id, sys_prov_acip_info.ac_city_name, sys_prov_acip_info.ac_name, sys_prov_acip_info.ac_ip, sys_prov_acip_info.create_time, sys_prov_acip_info.update_time
   FROM sys_prov_acip_info;


CREATE OR REPLACE FUNCTION clean_data(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text;
	v_table_name text;
	v_end_pname text;
	v_partitionname text;
	v_func_name text:='clean_data';
	p_partition_cur refcursor;

begin
	---------数据清理过程，注意这个过程需要在当天所有的ETL执行完毕之后执行---------
	perform write_runlog(v_func_name,'function start',0);
	--保留一天的数据清理
	--外部表日志表
	perform write_runlog(v_func_name,'delete err_ext_apache_log start',0);
	delete from err_ext_apache_log where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete err_ext_cboss_log start',0);
	delete from err_ext_cboss_log where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete err_ext_monitor_log start',0);
	delete from err_ext_monitor_log where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete err_ext_radius_log start',0);
	delete from err_ext_radius_log where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete err_ext_subscription start',0);
	delete from err_ext_subscription where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete err_ext_usage start',0);
	delete from err_ext_usage where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete err_ext_wlan_package start',0);
	delete from err_ext_wlan_package where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete err_ext_wlan_user_cookie start',0);
	delete from err_ext_wlan_user_cookie where cmdtime <to_date(vi_dealdate,'yyyy-mm-dd')-1;

	--一般表
	perform write_runlog(v_func_name,'delete temp_cha_err start',0);
	delete from temp_cha_err where date_time <to_date(vi_dealdate,'yyyy-mm-dd')-1;
	perform write_runlog(v_func_name,'delete src_apache_log start',0);
	delete from src_apache_log where odate<to_date(vi_dealdate,'yyyy-mm-dd')-1;

	--一个月，在每个月1号执行
	if to_date(vi_dealdate,'yyyy-mm-dd')=to_date(substr(vi_dealdate,1,7),'yyyy-mm') then
		v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') - interval '31 day', 'yyyy-mm-dd');
		v_end_pname = 'prt_' || replace(v_end_date, '-','');

		perform write_runlog(v_func_name,'delete src_apache_month_log start',0);
		v_table_name='src_apache_month_log';
		open p_partition_cur for execute 'select partitionname from pg_partitions where tablename=''' || v_table_name || ''' and partitionname<''' || v_end_pname ||'''';
			loop 
				fetch 
					p_partition_cur 
				into 
					v_partitionname;
				exit when v_partitionname is null;
				execute ' alter table '|| v_table_name ||' drop partition ' || v_partitionname || ';';
			end loop;
		close p_partition_cur;
		perform write_runlog(v_func_name,'delete temp_login_request_success start',0);
		delete from temp_login_request_success where date_time <to_date(vi_dealdate,'yyyy-mm-dd')-31;
		perform write_runlog(v_func_name,'delete temp_online_user_ip start',0);
		--delete from temp_online_user_ip where odate
		v_table_name='temp_online_user_ip';
		open p_partition_cur for execute 'select partitionname from pg_partitions where tablename=''' || v_table_name || ''' and partitionname<''' || v_end_pname ||'''';
			loop 
				fetch 
					p_partition_cur 
				into 
					v_partitionname;
				exit when v_partitionname is null;
				execute ' alter table '|| v_table_name ||' drop partition ' || v_partitionname || ';';
			end loop;
		close p_partition_cur;
	end if;

	--半年,每个月1号检查执行
	if to_date(vi_dealdate,'yyyy-mm-dd')=to_date(substr(vi_dealdate,1,7),'yyyy-mm') then
		v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') - interval '186 day', 'yyyy-mm-dd');
		v_end_pname = 'prt_' || replace(v_end_date, '-','');
		perform write_runlog(v_func_name,'delete src_monitor_log start',0);
		v_table_name='src_monitor_log';
		open p_partition_cur for execute 'select partitionname from pg_partitions where tablename=''' || v_table_name || ''' and partitionname<''' || v_end_pname ||'''';
			loop 
				fetch 
					p_partition_cur 
				into 
					v_partitionname;
				exit when v_partitionname is null;
				execute ' alter table '|| v_table_name ||' drop partition ' || v_partitionname || ';';
			end loop;
		close p_partition_cur;

		perform write_runlog(v_func_name,'delete src_radius_log start',0);
		v_table_name='src_radius_log';
		open p_partition_cur for execute 'select partitionname from pg_partitions where tablename=''' || v_table_name || ''' and partitionname<''' || v_end_pname ||'''';
			loop 
				fetch 
					p_partition_cur 
				into 
					v_partitionname;
				exit when v_partitionname is null;
				execute ' alter table '|| v_table_name ||' drop partition ' || v_partitionname || ';';
			end loop;
		close p_partition_cur;

		perform write_runlog(v_func_name,'delete src_subscription start',0);
		delete from src_subscription where bms_create_time <to_date(vi_dealdate,'yyyy-mm-dd')-31*6;
		perform write_runlog(v_func_name,'delete src_usage start',0);
		v_table_name='src_usage';
		open p_partition_cur for execute 'select partitionname from pg_partitions where tablename=''' || v_table_name || ''' and partitionname<''' || v_end_pname ||'''';
			loop 
				fetch 
					p_partition_cur 
				into 
					v_partitionname;
				exit when v_partitionname is null;
				execute ' alter table '|| v_table_name ||' drop partition ' || v_partitionname || ';';
			end loop;
		close p_partition_cur;
		perform write_runlog(v_func_name,'delete src_wlan_package start',0);
		delete from src_wlan_package where time_stamp<to_date(vi_dealdate,'yyyy-mm-dd')-31*6;
		perform write_runlog(v_func_name,'delete src_wlan_user_cookie start',0);
		delete from src_wlan_user_cookie where time_stamp<to_date(vi_dealdate,'yyyy-mm-dd')-31*6;
	end if;


	perform write_runlog(v_func_name,'function end',0);
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;