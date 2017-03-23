# pickyport
Python 2.7 script for porting databases with only selected amounts of data included. Currently only supports MySQL, but contributions welcome for other database types (Postgres, Oracle, etc)

## Requirements
* Python 2.7
** PyYAML module
* mysql
* mysqldump

## Configuration
Configuration file should be in the following *.yaml format:

```
portages:
 
 - db_type: mysql
   fetch_data [true|false]
   ignore_tables: [A, B, C]
   create_dest_db: [true|false]
   test_users:
     - permissions: [write|read|admin]
       user: test_user
       password: test_password
 
   source:
     host: source_host
     user: source_user
     password: source_password
     name: source_database_name
   
   dest: 
     - host: dest_host_1
       user: dest_user_1
       password: dest_pass_1
       name: dest_db_name_1
       
     - host: dest_host_2
       user: dest_user_2
       password: dest_pass_2
       name: dest_db_name_2
     
   update: /this/is/my/update.sql
```

## Usage

`python pickyport.py [-h] [-q] [-X] [-d] config_file`

optional arguments:

|---|---|
| -h | --help     | show this help message and exit
| -q | --quiet    | run with no output
| -X | --debug    | show parsed config, all commands, and save temp files
| -d | --dry-run  | show commands without running them
