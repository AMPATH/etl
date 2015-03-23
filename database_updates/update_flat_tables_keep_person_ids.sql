#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE}")" ; pwd -P )
cd "$parent_path"
mysql reporting_JD < ../flat_tables/flat_arvs.sql
mysql reporting_JD < ../flat_tables/flat_drug.sql
mysql reporting_JD < ../flat_tables/flat_encounter.sql
mysql reporting_JD < ../flat_tables/flat_ext_data.sql
mysql reporting_JD < ../flat_tables/flat_handp.sql
mysql reporting_JD < ../flat_tables/flat_int_data.sql
mysql reporting_JD < ../flat_tables/flat_maternity.sql
mysql reporting_JD < ../flat_tables/flat_peds.sql
mysql reporting_JD < ../flat_tables/flat_tb.sql
