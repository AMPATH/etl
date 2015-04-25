#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE}")" ; pwd -P )
cd "$parent_path"
mysql reporting_JD < ../flat_tables/flat_obs.sql
