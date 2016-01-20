#!/bin/bash
set -e
if [ ! -f /.postgres_db_configured ]; then
	/set_postgres_db.sh
fi

if [ ! -f /.tomcat_configured ]; then
	/set_tomcat.sh
fi

echo "=> Starting and Running Tomcat..."
/usr/local/tomcat/bin/catalina.sh run
