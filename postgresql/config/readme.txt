简易安装说明:
1. 在后台以postgres用户登陆
  登陆语句如：psql （其中 -p后的password指密码 ）
登陆成功后执行下面语句：
create user aiwlanbi with password aiwlanbi;

create user aiwlanbi with NOSUPERUSER NOCREATEROLE PASSWORD 'aiwlanbi';
CREATE DATABASE aiwlanbi WITH owner=aiwlanbi ENCODING 'UTF8';
执行完后即创建了一个用户名为aiwlanbi,密码为aiwlanbi的用户。

2.在安装脚本所在机器远程连接数据库，进入aiwlanbi目录下执行登陆GP语句如：
psql -h localhost -U aiwlanbi
登陆成功后执行如下语句：

\i install.sql;

3.卸载数据库：
psql -U postgres

\i uninstall.sql;









