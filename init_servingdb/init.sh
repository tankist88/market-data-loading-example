#!/bin/bash

export PGPASSWORD=password123
psql servingdb postgres -f /docker-entrypoint-initdb.d/init_servingdb.sql