/*创建三个默认角色*/
delete from sys_role;
insert into sys_role (ROLE_ID, ROLE_NAME, ROLE_STATUS, ROLE_TYPE, CREATE_TIME, UPDATE_TIME, EDITABLE, REMARK) values (1, '系统管理员', 0, 1, now(), null, 'N', '系统管理员');
insert into sys_role (ROLE_ID, ROLE_NAME, ROLE_STATUS, ROLE_TYPE, CREATE_TIME, UPDATE_TIME, EDITABLE, REMARK) values (2, '集团管理员', 0, 2, now(), null, 'N', '集团管理员');
insert into sys_role (ROLE_ID, ROLE_NAME, ROLE_STATUS, ROLE_TYPE, CREATE_TIME, UPDATE_TIME, EDITABLE, REMARK) values (3, '省管理员', 0, 3, now(), null, 'N', '省管理员');