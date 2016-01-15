/*给三种角色类型初始化不同的权限*/
delete from sys_func_role;

INSERT INTO sys_func_role(role_id, function_id)
SELECT 1, function_id FROM sys_function ORDER BY function_id;

INSERT INTO sys_func_role(role_id, function_id)
select 2, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_pid not in(7,71,711,712);

INSERT INTO sys_func_role(role_id, function_id)
select 3, FUNCTION_ID from sys_function a where a.function_id != 7 and a.function_id != 3 and a.function_pid not in(7,71,711,712);