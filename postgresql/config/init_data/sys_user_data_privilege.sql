/*给三个用户赋省份权限*/
delete from sys_user_data_privilege;
insert into sys_user_data_privilege (user_id, prov_code)
select 1 ,prov_id from sys_prov_info;
insert into sys_user_data_privilege (user_id, prov_code)
select 2 ,prov_id from sys_prov_info;
insert into sys_user_data_privilege (user_id, prov_code)
values(3,20);