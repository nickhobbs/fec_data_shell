import csv
import datetime
import copy
import sqlite3
import sys
import cmd
import os
import re

DEFAULT_TABLE_DEFINITION_PATH = "table_definition.csv"
TABLE_NAME = "FEC_data.db"
DATA_LAST_UPDATED = datetime.datetime(year=2018, month=3, day=25)

def main():
    FECShell().cmdloop()

class FECShell(cmd.Cmd):
    intro = "Welcome to the FEC data shell. Type help or ? to list commands"
    prompt = "(FEC query) "
    macros = dict()

    MACRO_PATTERN = "\$\w+"
    MACRO_FILE_NAME = "macros.csv"

    def __init__(self, *args, **kwargs):
        """Creates the connection to the database the shell will use and loads 
        macros from disk."""
        cmd.Cmd.__init__(self, *args, **kwargs)
        self.connection = sqlite3.connect(TABLE_NAME)
        self.load_macros()

    def do_init(self, arg):
        """Imports data defined in table_definition.csv'. Takes optional 
        argument that changes the table definition path.
        """

        ## If the file exists, ask the user for premission to delete it.
        if os.path.isfile(TABLE_NAME):
            print "WARNING: database already exists. Continuing will delete it."
            continue_prompt = "Do you want to continue? (y or n)\n"
            response = self.validated_input(continue_prompt, ["y", "n"])
            if response == "n":
                print "Initilization stopped."
                return

            os.remove(TABLE_NAME)
            self.connection = sqlite3.connect(TABLE_NAME)
        
        path = ""
        if len(arg) > 0:
            path = arg
        else:
            path = DEFAULT_TABLE_DEFINITION_PATH
        init_database(self.connection, path)

    def do_query(self, arg):
        "run a SQL query against the database"

        if len(arg) < 1:
            print "ERROR: query requires queryStr as the second argument"
            return

        # Replace macros in the string
        macros_in_query = re.findall(self.MACRO_PATTERN, arg)
        for macro in macros_in_query:
            if macro in self.macros:
                arg = re.sub(self.MACRO_PATTERN,
                             self.macros[macro],
                             arg,
                             count=1)
            else:
                print "SyntaxError: '" + macro + "' is not defined."
                print "Use command 'macros' to list all defined macros"
                return
        
        print "running query: '" + arg + "'..."
        cursor = self.connection.cursor()
        try:
            cursor.execute(arg)
        except sqlite3.OperationalError as error:
            print "ERROR: " + str(error)
            return
        query_result = cursor.fetchall()
        for line in query_result:
            print str(line)
        print "\n"

    def do_macro(self, arg):
        """Creates a macro. First argumet is the macro name of form $<name>. 
        Second argument is the string the macro will expand to"""

        macro_name_results = re.findall(self.MACRO_PATTERN, arg)
        if len(macro_name_results) < 1:
            print "SyntaxError: first argument must be '$<macro_name>'"
            return
        macro_name = macro_name_results[0]
        
        macro_value_results = re.findall('".*"', arg)
        if len(macro_value_results) < 1:
            print 'SyntaxError: second argument must be a "" wrapped string'
            return
        macro_value = macro_value_results[0].strip('"')

        self.macros[macro_name] = macro_value
        self.save_macros()
        print ("created macro with " + macro_name +
               ' with value "' + macro_value + '"')

    def do_macros(self, arg):
        """Prints all current macros"""
        print "there are", len(self.macros), "macros defined:"
        for macro_name in self.macros:
            print macro_name, '"' + self.macros[macro_name] + '"'

    def save_macros(self):
        """Saves self.macros to macros.csv file."""
        with open(self.MACRO_FILE_NAME, "wb") as macro_file:
            macro_writer = csv.writer(macro_file)
            for macro_name in self.macros:
                macro_writer.writerow([macro_name, self.macros[macro_name]])

    def load_macros(self):
        """Replaces self.macros with data from the macros.csv file. Each row is
        formatted as $macro_name,macro_value"""
        if not os.path.isfile(self.MACRO_FILE_NAME):
            self.macros = dict()
            self.save_macros()
            return
        with open(self.MACRO_FILE_NAME, "rb") as macro_file:
            self.macros = dict()
            macro_reader = csv.reader(macro_file)
            for row in macro_reader:
                self.macros[row[0]] = row[1]

    def validated_input(self, prompt, options):
        """Prompts the user until they respond with input that matches one of
        the strings in 'options'.

        Args:
            prompt: string presented to user as a prompt
            options: list of strings that are valid responses
        """
        
        response = raw_input(prompt)
        if response in options:
            return response

        invalid_response_str = "Invalid response. Expected one of: "
        for option in options:
            invalid_response_str += "'" + option + "'" + ", "
        invalid_response_str = invalid_response_str.rstrip(", ") + "."
        print invalid_response_str
        self.validated_input(prompt, options)

def init_database(connection, db_definition_path):
    """" Adds tables to a SQLite database from the specified file.

    Args:
        connection: connection to the SQLite database tables will be added to
        db_definition_path: path to a csv file that specifies the data source
            files, header files, and table name.

    Side Effects:
        adds tables and records to the database specified by connection
    """
    cursor = connection.cursor()
    
    with open(db_definition_path, "rb") as tableDefinitionFile:
        # Open the table definition file and read the first line (header).
        table_definition_reader = csv.reader(tableDefinitionFile)
        table_definition_reader.next()
        
        for dataSourceDef in table_definition_reader:
            data_file_path = dataSourceDef[0]
            data_file_delimiter = dataSourceDef[1]
            header_file_path = dataSourceDef[2]
            header_file_delimiter = dataSourceDef[3]
            table_name = dataSourceDef[4]
            
            add_data_to_table(data_file_path, data_file_delimiter,
                              header_file_path, header_file_delimiter,
                              table_name, cursor)

def add_data_to_table(data_file_path, data_file_delimiter, header_file_path,
                      header_delimiter, table_name, database_cursor):
    """Adds data to the database from files on disk.

    Args:
        data_file_path: string specifying the path of the data file.
        data_file_delimiter: string specifying the delimiter of the data file.
        header_file_path: string specifying the path to the header file. First
            of the file should specify field names & the second field types.
        header_delimiter: string specifying the header file delimiter.
        table_name: string specifying the name of the table to add data to
        database_cursor: SQLite cursor for the database to add to
    """
    
    with open(header_file_path, "rb") as header_file:
        print "importing data from: " + data_file_path
        header_reader = csv.reader(header_file, delimiter=header_delimiter)
        field_names = header_reader.next()
        field_types = header_reader.next()

        table_check_str = ("select name from sqlite_master where " +
                         "type='table' and name='" + table_name + "';")
        database_cursor.execute(table_check_str)
        query_result = database_cursor.fetchone()
        if (query_result == None):
            create_table(table_name, field_names, field_types, database_cursor)

        insert_str = "insert into " + table_name + " values("
        insert_str += ("?," * len(field_types))
        insert_str = insert_str.rstrip(",") + ")"

        with open(data_file_path, "rb") as data_file:
            data_reader = csv.reader(data_file, delimiter = data_file_delimiter)
            for record in data_reader:
                values = parse_record(record, field_types, DATA_LAST_UPDATED)
                if values == None:
                    continue
                try:
                    database_cursor.execute(insert_str, values)
                except sqlite3.OperationalError as e:
                    print "query '" + insert_str + "' failed."
                    print "ERROR: " + str(e)
                    raise e
            database_cursor.connection.commit()
            print "added data"

def create_table(table_name, field_names, field_types, database_cursor):
    """Creates a table in the specified database.

    Args:
        table_name: string specifying the table's name
        field_names: list of strings for each field's name
        field_types: list of strings specifying each field's SQLite type.
        database_cursor: SQLite cursor object for the database to add to
    """
    
    create_table_str = "create table " + table_name + "("
    field_number = 0
    for field in field_names:
        fieldType = field_types[field_number]
        field_number += 1
        create_table_str += field + " " + fieldType + ", "
    create_table_str = create_table_str.rstrip(", ") + ");"
    database_cursor.execute(create_table_str)
    database_cursor.connection.commit()
    

def parse_record(record, field_types, max_date):
    """Converts a csv record into the proper values.

    Args:
        record: a csv record of strings
        field_types: strings for the SQLite type of each field (text, integer, 
            or date are the only currently supported types)
        max_date: datetime, if date fields are created after max_date, None will
            be returned
    Return:
        Returns a list with proper python values (str, int, or datetime) unless
        a date value exceeds max_date. Then, None is returned."""
    
    values = []
    field_number = 0
    for value in record:
        value_type = field_types[field_number]
        field_number += 1
        if value_type == "text":
            values.append(value)
        elif value_type == "integer":
            values.append(int(value))
        elif value_type == "date":
            date = datetime.datetime.strptime(value, "%m%d%Y")
            if date > max_date:
                return None
            values.append(date)
    return values

if __name__ == "__main__":
   main()
