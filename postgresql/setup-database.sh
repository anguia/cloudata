#!/bin/bash
#TEST=`gosu postgres postgres --single <<- EOSQL
#SELECT 1 FROM pg_database WHERE datname='$DB_NAME';
#EOSQL`
TEST=1
if [[ $TEST == "1" ]]; then
	# database exists
	# $? is 0
	exit 0
else
	echo "******CREATING DOCKER DATABASE******"
	gosu postgres postgres --single <<- EOSQL
	CREATE ROLE $DB_USER WITH LOGIN ENCRYPTED PASSWORD '${DB_PASS}' CREATEDB;
	EOSQL
	gosu postgres postgres --single <<- EOSQL
	CREATE DATABASE $DB_NAME WITH OWNER $DB_USER TEMPLATE template0 ENCODING 'UTF8';
	EOSQL
	gosu postgres postgres --single <<- EOSQL
	GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
	EOSQL
	cd /docker-entrypoint-initdb.d/config
	psql -U$DB_USER -d$DB_NAME < install.sql
	cd -
fi
