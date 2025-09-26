sqlite3 db.db <<EOF
.mode csv
.output commits.csv
select * from commits;
.output by_file.csv
select * from by_file;
.output by_code.csv
select * from by_code;
EOF
