truncate table sys_func_role;
INSERT INTO sys_func_role(role_id, function_id)
SELECT 1, function_id FROM sys_function;

INSERT INTO sys_func_role(role_id, function_id)
SELECT 2, function_id FROM sys_function;

INSERT INTO sys_func_role(role_id, function_id)
SELECT 3, function_id FROM sys_function;