#!/bin/bash

# Ensure a year is provided
YEAR=$1
if [ -z "$YEAR" ]; then
    echo "Usage: ./init_season.sh <YYYY>"
    exit 1
fi

# Define database credentials (or export these to your EC2 environment beforehand)
DB_ENDPOINT="lotwdb.cuquk2hic4gt.us-west-2.rds.amazonaws.com"
DB_PORT="3306"
DB_USERNAME="lotw"
DB_NAME="lotw"

echo "Generating schema for $YEAR..."

# Replace {YEAR} in the template and save to a temporary file
sed "s/{YEAR}/$YEAR/g" /tmp/lotw_database_schema_template.sql > /tmp/lotw_schema_$YEAR.sql

echo "Applying schema to $DB_ENDPOINT..."

# Pipe the generated SQL into the MySQL client
mysql -h "$DB_ENDPOINT" -P "$DB_PORT" -u "$DB_USERNAME" -p "$DB_NAME" < /tmp/lotw_schema_$YEAR.sql

if [ $? -eq 0 ]; then
    echo "SUCCESS: Schema for $YEAR initialized."
else
    echo "ERROR: Failed to apply schema."
fi

# Clean up
rm /tmp/lotw_schema_$YEAR.sql
