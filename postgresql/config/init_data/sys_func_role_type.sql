/*给三种角色类型初始化不同的菜单权限选择项*/
delete from sys_func_role_type;
insert into sys_func_role_type
select 1, FUNCTION_ID from sys_function;
insert into sys_func_role_type(role_type,function_id)
select 2, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_pid not in(7,71,711,712);
insert into sys_func_role_type(role_type,function_id)
select 3, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_id != 3 and a.function_pid not in(7,71,711,712);