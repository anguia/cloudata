#!/bin/bash
set -e
if [ -f /.postgres_db_configured ]; then
	echo "=> PostgresDB has been configured!"
	exit 0
fi

if [ "${POSTGRES_HOST}" = "**ChangeMe**" ]; then
	echo "=> No address of PostgresDB is specified!"
	echo "=> Porgram terminated!"
	exit 1
fi

echo "=> Configuring PostgresDB"
touch /.postgres_db_configured
echo "=> PostgresDB has been configured as follows:"
echo "   PostgresDB ADDRESS:  ${POSTGRES_HOST}"
echo "   PostgresDB PORT:     ${POSTGRES_PORT}"
echo "   PostgresDB DB NAME:  ${POSTGRES_DB_NAME}"
echo "   PostgresDB USERNAME: ${POSTGRES_USER}"
echo "   PostgresDB PASSWORD: ${POSTGRES_PASS}"
echo "   ** Please check your environment variables if you find something is misconfigured. **"
echo "=> Done!"
