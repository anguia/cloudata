/*系统操作日志内容明显*/
delete from sys_action_type;

insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (100,11,'用户登录');
--insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (101,11,'注销登录');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (111,11,'增加用户');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (112,11,'删除用户');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (113,11,'修改用户信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (117,11,'修改密码');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (118,11,'重置密码');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (119,11,'修改用户个人信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (131,13,'增加角色');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (132,13,'删除角色');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (133,13,'修改角色');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (134,13,'修改角色授权');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (135,13,'修改数据查看权限');

insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (231,23,'增加省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (232,23,'修改省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (233,23,'删除省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (234,23,'批量导入省内地址段信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (281,28,'增加ACIP信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (282,28,'修改ACIP信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (283,28,'删除ACIP信息');
insert  into sys_action_type(ACTION_TYPE_ID,operate_obj_type_id,name) values (284,28,'批量导入ACIP信息');