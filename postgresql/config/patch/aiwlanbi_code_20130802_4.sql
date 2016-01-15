DROP TABLE rpt_cboss_monitor_day;

CREATE TABLE rpt_cboss_monitor_day
(
  odate timestamp without time zone, -- 日志时间
  bip_code text, -- 业务受理代码
  trans_id text, -- 流水号
  biz_type text, -- 业务类型代码：...
  opr_code text, -- 操作代码...
  user_name text, -- 用户账号
  sp_biz_code text, -- 套餐编号代码:...
  user_status text, -- 用户状态代码：...
  process_time timestamp without time zone, -- 处理时间
  opr_time timestamp without time zone, -- 套餐申请/取消/变更操作时间
  efft_time timestamp without time zone, -- 套餐申请/取消/变更生效时间
  rsp_desc text, -- 处理结果描述
  orig_domain text, -- 业务发起方
  home_prov integer -- 省份代码
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (odate)
PARTITION BY RANGE(odate) 
          (
          PARTITION prt_20130701 START ('2013-07-01 00:00:00'::timestamp without time zone) END ('2013-07-02 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          PARTITION prt_20130710 START ('2013-07-10 00:00:00'::timestamp without time zone) END ('2013-07-11 00:00:00'::timestamp without time zone) WITH (appendonly=true, compresslevel=5, compresstype=zlib)
          )
;

COMMENT ON COLUMN rpt_cboss_monitor_day.odate IS '日志时间';
COMMENT ON COLUMN rpt_cboss_monitor_day.bip_code IS '业务受理代码';
COMMENT ON COLUMN rpt_cboss_monitor_day.trans_id IS '流水号';
COMMENT ON COLUMN rpt_cboss_monitor_day.biz_type IS '业务类型代码：
02-WLAN（大众业务）
92-WLAN（高校业务）';
COMMENT ON COLUMN rpt_cboss_monitor_day.opr_code IS '操作代码
01-用户注册
02-用户注销 
03-密码重置
04-用户暂停 
05-用户恢复
06-服务订购
07-服务订购取消 
10-套餐申请 
11-套餐取消 
12-套餐变更
14-达到封顶后的暂停 
15-封顶暂停后的恢复
16-该套餐用尽后的通知 
17-该套餐用尽后的恢复
';
COMMENT ON COLUMN rpt_cboss_monitor_day.user_name IS '用户账号';
COMMENT ON COLUMN rpt_cboss_monitor_day.sp_biz_code IS '套餐编号代码:
基础功能，标准资费（每分钟0.05元）	00000	
WLAN无线宽带上网套餐30元包月	00001	
WLAN无线宽带上网套餐50元包月	00002	
WLAN无线宽带上网套餐100元包月	00003	
WLAN无线宽带上网套餐5元包月	00004	
WLAN无线宽带上网套餐10元包月	00005	
WLAN无线宽带上网套餐20元包月	00006	
WLAN无线宽带上网套餐200元包月	00007	
WLAN包月不限时资费套餐10元	00011	
WLAN包月不限时资费套餐20元	00012	
WLAN包月不限时资费套餐50元	00013	
WLAN包月不限时资费套餐叠加包10元	00014	
高校WLAN优惠套餐10元包月	10001	
高校WLAN优惠套餐20元包月	10002	
高校WLAN优惠套餐40元包月	10003	
WLAN包单位时间资费包小时	20001	
WLAN包单位时间资费包天	20002	
WLAN包单位时间资费包周	20003	
WLAN包单位时间资费包月	20004	
WLAN包时长资费30元/15小时	30001	
WLAN包时长资费50元/40小时	30002	
WLAN包时长资费100元/200小时	30003';
COMMENT ON COLUMN rpt_cboss_monitor_day.user_status IS '用户状态代码：
00	正常
01	单向停机
02	停机
03	预销户
04	销户
05	过户
06	改号
10	智能网用户有效期
11	智能网用户保留期
12	智能网用户冷冻期
90	神州行用户
99	此号码不存在';
COMMENT ON COLUMN rpt_cboss_monitor_day.process_time IS '处理时间';
COMMENT ON COLUMN rpt_cboss_monitor_day.opr_time IS '套餐申请/取消/变更操作时间';
COMMENT ON COLUMN rpt_cboss_monitor_day.efft_time IS '套餐申请/取消/变更生效时间';
COMMENT ON COLUMN rpt_cboss_monitor_day.rsp_desc IS '处理结果描述';
COMMENT ON COLUMN rpt_cboss_monitor_day.orig_domain IS '业务发起方';
COMMENT ON COLUMN rpt_cboss_monitor_day.home_prov IS '省份代码';

CREATE OR REPLACE FUNCTION etl_rpt_cboss_monitor_day(vi_dealdate text)
  RETURNS void AS
$BODY$
declare
	v_end_date text; --结束日期 格式：yyyy-mm-dd
	v_partition_name text; --表分区名称
begin

	v_end_date = to_char(to_date(vi_dealDate, 'yyyy-mm-dd') + interval '1 day', 'yyyy-mm-dd');
	v_partition_name = 'prt_' || replace(vi_dealDate, '-','');

	--检查表分区是否存在，不存在则新建，存在则删除
	if exists(select 1 from pg_partitions where lower(tablename)=lower('etl_rpt_cboss_monitor_day') and partitionname = v_partition_name) then
		execute ' alter table rpt_cboss_monitor_day truncate partition ' || v_partition_name || ';';
	else 
		execute ' alter table rpt_cboss_monitor_day add partition ' || v_partition_name || ' start (date ''' 
			|| vi_dealDate || ''') end (date ''' || v_end_date || ''') WITH (appendonly=true, compresslevel=5, compresstype=zlib);';
	end if;

	--从外部表导入cboss数据，只导入BIP2B147的
	insert into rpt_cboss_monitor_day(odate, bip_code, trans_id, biz_type, opr_code, user_name, sp_biz_code, 
            user_status, process_time, opr_time, efft_time, rsp_desc, orig_domain, home_prov)
	select to_timestamp(a.arr[1]||' '||a.arr[2],'yyyy/mm/dd hh24:mi:ss'),a.bip_code,a.trans_id,a.biz_type,a.opr_code,a.user_name,a.sp_biz_code,a.user_status,
	       a.process_time,a.opr_time,a.efft_time,a.rsp_desc,a.orig_domain,a.home_prov
	from (
		select string_to_array(part1,' ') arr,
		       trim(part2) bip_code,
		       trim(part4) trans_id,
		       trim(part5) biz_type,
		       trim(part6) opr_code,
		       trim(part7) user_name,
		       trim(part8) sp_biz_code,
		       trim(part9) user_status,
		       to_timestamp(trim(part11),'yyyymmddhh24miss') process_time,
		       to_timestamp(trim(part12),'yyyymmddhh24miss') opr_time,
		       to_timestamp(trim(part13),'yyyymmddhh24miss') efft_time,
		       trim(part15) rsp_desc,
		       trim(part17) orig_domain,
		       trim(part20)::integer home_prov
		from ext_cboss_log 
		where trim(part2) = 'BIP2B147'
        ) a;
end;
$BODY$
  LANGUAGE plpgsql VOLATILE;