#!/usr/local/bin/python
"""
pickyport.py

This script uses a yaml configuration file to generate a list of commands
which will port the schema and selected data from a source database into
one or more destination databases. The script can also create the destination
database and any test users indicated in the configuration.

Written by: Amber Race (ambertests)
Date: March 22, 2017

"""
import os
import yaml
import argparse
from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE


def which(program):
    """
    Utility method for finding a given executable, similar to the unix 'which' util.
    http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python/377028#377028
    """

    def is_exe(abs_path):
        return os.path.isfile(abs_path) and os.access(abs_path, os.X_OK)

    path, name = os.path.split(program)

    if path:
        if is_exe(program):
            return program
    else:
        for os_path in os.environ["PATH"].split(os.pathsep):
            os_path = os_path.strip('"')
            exe_file = os.path.join(os_path, program)
            if is_exe(exe_file):
                return exe_file
    return None


class BasePorter(object):
    """
    The Porter is the object which creates and runs the necessary commands for porting the source database
    """
    def __init__(self):
        self.quiet = False
        self.debug = False
        self.dry_run = False
        self.source = None
        self.dest = []
        self.test_users = []
        self.create_dest_db = False
        self.fetch_data = True
        self.ignore_tables = []
        self.temp_files = []
        self.update_scripts = []

    def set_variables(self, port_info, quiet, debug, dry_run):
        """
        :param port_info: source and destination info from the configuration file
        :param quiet: if true, don't print anything to the console
        :param debug: if true, print everything to the console and save temp files
        :param dry_run: if true, just print the commands without actually running them
        :return:
        """
        self.quiet = quiet
        self.debug = debug
        self.dry_run = dry_run

        self.source = port_info['source']
        self.dest = port_info['dest']
        if type(self.dest) is not list:
            self.dest = [self.dest]

        if 'create_dest_db' in port_info:
            self.create_dest_db = port_info['create_dest_db']

        if 'test_users' in port_info:
            self.test_users = port_info['test_users']

        if 'fetch_data' in port_info:
            self.fetch_data = port_info['fetch_data']

        if 'ignore_tables' in port_info:
            self.ignore_tables = port_info['ignore_tables']

        if 'update' in port_info:
            self.update_scripts = port_info['update']

    def get_temporary_file(self):
        """
        Makes a temporary file to use for receiving data from the source database
        and loading that data into the destination databases.
        :return:The name of the temporary file
        """
        temp_file = NamedTemporaryFile(suffix='.sql', delete=False)
        file_name = temp_file.name
        self.temp_files.append(file_name)
        temp_file.close()
        return file_name

    def create_db_commands(self):
        """
        Makes the necessary command lines for creating the destination databases
        :return:a list of tuples containing:
        * description to print to console
        * actual command
        """
        description = "This command creates a new database on the destination host"
        command = "This is the actual command"
        return description, command

    def create_grant_commands(self):
        """
        Makes the command lines for adding test users to the destination databases
        :return:a list of tuples containing:
        * description to print to console
        * actual command
        """
        commands = []
        description = "This command creates a user in the destination database"
        command = "This is the actual command"
        commands.append((description, command))
        return commands

    def create_dump_command(self, schema_only, sql_file):
        """
        Makes the necessary command line for dumping schema and/or data from the source database
        :return:a single tuple containing:
        * description to print to console
        * actual command
        """
        description = "This command dumps data from the source database into the sql_file"
        command = "This is the actual command"
        return description, command

    def create_load_commands(self, sql_file):
        """
        Makes the necessary command line for loading schema and/or data into the source databases
        :return:a list of tuples containing:
        * description to print to console
        * actual command
        """
        commands = []
        description = "This loads data and/or tables from the sql_file into the source database"
        command = "This is the actual command"
        commands.append((description, command))
        return commands

    def create_update_commands(self):
        """
        Makes the necessary command lines for applying updates to the destination databases
        :return:a list of tuples containing:
        * description to print to console
        * actual command
        """
        commands = []
        description = "This command creates a new database on the destination host"
        command = "This is the actual command"
        commands.append((description, command))
        return commands

    def do_portage(self):
        """
        Main method for creating and running all the commands needed to complete a port
        :return:
        """
        cmd_list = []

        if self.create_dest_db:
            cmd_list.extend(self.create_db_commands())

        if self.test_users:
            cmd_list.extend(self.create_grant_commands())

        if self.fetch_data:
            if self.ignore_tables:
                schema_file = self.get_temporary_file()
                data_file = self.get_temporary_file()
                cmd_list.append(self.create_dump_command(True, schema_file))
                cmd_list.extend(self.create_load_commands(schema_file))
                cmd_list.append(self.create_dump_command(False, data_file))
                cmd_list.extend(self.create_load_commands(data_file))
            else:
                schema_data_file = self.get_temporary_file()
                cmd_list.append(self.create_dump_command(False, schema_data_file))
                cmd_list.extend(self.create_load_commands(schema_data_file))
        else:
            schema_file = self.get_temporary_file()
            cmd_list.append(self.create_dump_command(True, schema_file))
            cmd_list.extend(self.create_load_commands(schema_file))

        if self.update_scripts:
            cmd_list.extend(self.create_update_commands())

        if not self.quiet:
            if self.dry_run:
                print '===============\nStarting portage DRY RUN\n==============='
            else:
                print '===============\nStarting portage\n==============='

        for cmd in cmd_list:

            if not self.quiet:
                echo = cmd[0]
                print echo

            if self.debug or self.dry_run:
                print cmd[1]

            if not self.dry_run:
                proc = Popen(cmd[1], shell=True, stderr=PIPE)
                output = proc.communicate()
                if "ERROR" in output[1]:
                    # Don't duplicate the print if already in debug mode
                    if not self.debug:
                        print cmd[1]
                    print output[1]
                    # Remove the temp file used in the command from the
                    # list of temp files so it doesn't get deleted
                    for tf in self.temp_files:
                        if tf in cmd[1]:
                            self.temp_files.remove(tf)

            if not self.quiet:
                print '-------------------\n'

        if not self.debug:
            if not self.quiet:
                print 'Removing temp files...'
            for tf in self.temp_files:
                os.remove(tf)

        if not self.quiet:
            print 'Portage complete!\n\n'


class MySQLPorter(BasePorter):
    """
    The MySQL implementation of the BasePorter class
    """

    def __init__(self, port_info, quiet, debug, dry_run):
        """
        :param port_info: source and destination info from the configuration file
        :param quiet: if true, don't print anything to the console
        :param debug: if true, print everything to the console and save temp files
        :param dry_run: if true, just print the commands without actually running them
        :return:
        """
        BasePorter.__init__(self)
        self.mysqldump = which('mysqldump')
        if self.mysqldump is None:
            raise Exception("Must have mysqldump executable in PATH")
        self.mysql = which('mysql')
        if self.mysqldump is None:
            raise Exception("Must have mysql executable in PATH")
        self.set_variables(port_info, quiet, debug, dry_run)

    def create_dump_command(self, schema_only, output_file):
        """
        Creates the mysqldump command for getting data and/or schema out of the source database
        :param schema_only: If true, only get the schema, not any data
        :param output_file: File for storing the dumped schema and data
        :return:a single tuple containing:
        * description to print to console
        * actual command
        """
        if schema_only:
            # this will get all the CREATE TABLE
            # information without adding any data rows
            data_flag = '--no-data'
            dump_type = 'empty schema'
        elif self.ignore_tables:
            # this leaves out the create table info so the
            # data rows are bulk inserted into the pre-existing table
            data_flag = '--no-create-info --complete-insert'
            dump_type = 'selected data'
        else:
            # this will get the CREATE TABLE info and all the data at once
            data_flag = '--complete-insert'
            dump_type = 'all tables and data'

        ignored_tables = ''
        for ignore in self.ignore_tables:
            ignored_tables += '--ignore-table=%s.%s ' % (self.source['name'], ignore)

        dump_cmd = '%s --lock-tables=false --routines=true %s %s --result-file=%s ' % (self.mysqldump,
                                                                                       data_flag,
                                                                                       ignored_tables,
                                                                                       output_file)
        dump_cmd += '-h%s -u%s -p%s %s' % (self.source['host'],
                                           self.source['user'],
                                           self.source['password'],
                                           self.source['name'])
        dump_echo = 'Dumping %s from %s.%s...' % (dump_type, self.source['host'], self.source['name'])
        return dump_echo, dump_cmd

    def create_load_commands(self, input_file):
        """
        Creates the mysql commands for loading data and/or schema into the destination databases

        :param input_file: File with the data or schema to load
        :return:a list of tuples containing:
        * description to print to console
        * actual command
        """
        load_commands = []
        for dest in self.dest:
            load_cmd = ('%s -h%s -u%s -p%s %s < %s' % (self.mysql,
                                                       dest['host'],
                                                       dest['user'],
                                                       dest['password'],
                                                       dest['name'],
                                                       input_file))
            load_echo = 'Loading %s on %s.%s...' % (input_file, dest['name'], dest['host'])
            load_commands.append((load_echo, load_cmd))

        return load_commands

    def create_db_commands(self):
        """
        Creates the mysql command for creating new instances of the destination databases
        :return:a list of tuple containing:
        * description to print to console
        * actual command
        """
        create_commands = []
        for dest in self.dest:
            create_sql = 'DROP DATABASE IF EXISTS %s; CREATE DATABASE %s;' % (dest['name'], dest['name'])
            create_cmd = "%s -h%s -u%s -p%s -e '%s'" % (self.mysql,
                                                        dest['host'],
                                                        dest['user'],
                                                        dest['password'],
                                                        create_sql)
            create_echo = 'Creating %s on %s...' % (dest['name'], dest['host'])
            create_commands.append((create_echo, create_cmd))
        return create_commands

    def create_grant_commands(self):
        """
        Creates the mysql command for adding test users to the destination databases
        :return:a list of tuple containing:
        * description to print to console
        * actual command
        """
        grant_commands = []
        for test_user in self.test_users:
            if test_user['permissions'] == 'write':
                permissions = 'SELECT, INSERT, UPDATE, DELETE, EXECUTE'
            elif test_user['permissions'] == 'admin':
                permissions = 'ALL'
            else:
                permissions = 'SELECT'
            for dest in self.dest:

                grant_sql = "GRANT %s on %s.* to '%s'@'%%' IDENTIFIED BY '%s'; FLUSH PRIVILEGES;" % (permissions,
                                                                                                     dest['name'],
                                                                                                     test_user['user'],
                                                                                                     test_user['password'])
                grant_cmd = '%s -h%s -u%s -p%s -e "%s"' % (self.mysql,
                                                           dest['host'],
                                                           dest['user'],
                                                           dest['password'],
                                                           grant_sql)

                grant_echo = "Granting %s %s permission on %s.%s..." % (test_user['user'],
                                                                        test_user['permissions'],
                                                                        dest['host'],
                                                                        dest['name'])
                grant_commands.append((grant_echo, grant_cmd))
        return grant_commands

    def create_update_commands(self):
        """
        Makes the mysql command lines for applying updates to the destination databases
        :return:a list of tuples containing:
        * description to print to console
        * actual command
        """
        update_commands = []
        for update in self.update_scripts:
            if not os.path.isfile(update):
                print "ERROR: %s does not exist!" % update
                continue
            for dest in self.dest:
                create_cmd = "%s -h%s -u%s -p%s %s < %s" % (self.mysql,
                                                            dest['host'],
                                                            dest['user'],
                                                            dest['password'],
                                                            dest['name'],
                                                            update)
                create_echo = 'Applying %s to %s.%s...' % (update, dest['host'], dest['name'])
                update_commands.append((create_echo, create_cmd))
        return update_commands


def get_argument_parser():
    """
    Initializes command-line argument parser
    """

    arg_parser = argparse.ArgumentParser(description='Script for porting database with some or all of the source data.')
    arg_parser.add_argument('config', help='yaml-formatted configuration file')
    arg_parser.add_argument('-q', '--quiet', help='run with no output', required=False, default=False, action='store_true')
    arg_parser.add_argument('-X', '--debug', help='show parsed config, all commands, and save temp files', required=False,
                            default=False, action='store_true')
    arg_parser.add_argument('-d', '--dry-run', help='show commands without running them', required=False,
                            default=False, action='store_true')
    return arg_parser


if __name__ == '__main__':
    parser = get_argument_parser()
    args = parser.parse_args()
    config_file = args.config
    if not os.path.isfile(config_file):
        print config_file + " not found"
    else:
        try:
            cfg = yaml.safe_load(file(config_file, 'r'))

            if args.debug:
                from pprint import pprint
                pprint(cfg)

            for portage in cfg['portages']:

                if 'db_type' not in portage or portage['db_type'] == 'mysql':
                    porter = MySQLPorter(portage, args.quiet, args.debug, args.dry_run)
                else:
                    print "Only MySQL portages supported"
                    continue

                porter.do_portage()
        except yaml.YAMLError, ye:
            print "Could not parse configuration file"
            print ye
