#!/bin/bash

export PGPASSWORD=password123
psql tradesdb postgres -f /docker-entrypoint-initdb.d/init_tradesdb.sql