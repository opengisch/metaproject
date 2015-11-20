#!/bin/bash

set -e

export PGSERVICE=qwat

psql -c "DROP DATABASE IF EXISTS pg_test;"
psql -c "CREATE DATABASE pg_test;"

export PGSERVICE=pg_test

psql -v ON_ERROR_STOP=on -v SCHEMA=public -f pg_inherited_table_view.sql

psql -v ON_ERROR_STOP=on -f demo.sql
