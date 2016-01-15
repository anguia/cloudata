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