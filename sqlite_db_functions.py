import dbmangler_utils
import sqlite3
import json
import pytoml


def _dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class DB:
    def __init__(self, config_file_name='example_db_config.json'):
        if config_file_name[-5:] == '.toml':
            with open(config_file_name) as data_file:
                db_config = pytoml.load(data_file)
        else:
            with open(config_file_name) as data_file:
                db_config = json.load(data_file, object_hook=dbmangler_utils.decode_dict)
        self.schema = dbmangler_utils.DBSchema(db_config)
        self.db_name = 'default.db'
        self.con = sqlite3.connect(self.db_name)
        self.con.text_factory = str
        self.con.row_factory = _dict_factory
        self.cur = self.con.cursor()

    def run_select_command(self, command, values=None):
        if not values:
            try:
                self.cur.execute(command)
            except sqlite3.Error as e:
                print e
                return False
        else:
            try:
                self.cur.execute(command, values)
            except sqlite3.Error as e:
                print e
                return False

        return self.cur.fetchall()

    def run_edit_command(self, command, values):
        try:
            self.cur.execute(command, values)
        except sqlite3.Error as e:
            print e
            return False

        self.con.commit()

        return True

    def get_db_schema(self):
        command = "SELECT sql FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'"
        rows = self.run_select_command(command)  # type: list
        if not rows:
            return False
        ret_list = []
        for r in rows:
            ret_list.append(r['sql'])

        return ret_list

    def create_schema(self, return_script=False, return_array=False):
        create_tables_script = ""
        create_tables_array = []

        # for t in self.db_tables:
        for t in self.schema.tables:
            create_tables_script_line = "CREATE TABLE %s(" % t
            first_column = True
            foreign_key_in_table = []
            for column_name in self.schema.tables[t].columns:
                column_info = self.schema.tables[t].columns[column_name]
                if not first_column:
                    create_tables_script_line += ", "
                else:
                    first_column = False
                temp_column_info_type = list(column_info.type)
                if 'FOREIGN KEY' in column_info.type:
                    temp_column_info_type.remove('FOREIGN KEY')
                    foreign_key_in_table.append(column_name)
                column_type_string = " ".join(temp_column_info_type)
                create_tables_script_line += "%s %s" % (column_name, column_type_string)
            if foreign_key_in_table:
                for column_name in foreign_key_in_table:
                    column_info = self.schema.tables[t].columns[column_name]
                    create_tables_script_line += ", FOREIGN KEY(%s) REFERENCES %s(%s)" % \
                                                 (column_name, column_info.foreign_key['table'], column_name)

            create_tables_script_line += "); "
            create_tables_script += create_tables_script_line
            create_tables_array.append(create_tables_script_line[:-2])

        if not return_script and not return_array:
            self.cur.executescript(create_tables_script)
            self.con.commit()

            return True
        elif return_script:
            return create_tables_script
        elif return_array:
            return create_tables_array

    def check_schema(self, create_schema=True):
        existing_schema = self.get_db_schema()
        correct_schema = self.create_schema(return_array=True)

        if not existing_schema:
            if create_schema:
                print "No schema found. Creating DB schema."
                self.create_schema()
                return True
            else:
                print "No schema found."
                return False

        if len(existing_schema) < len(correct_schema):
            print "Wrong number of tables in DB, exiting."
            return False

        for s in correct_schema:
            if s not in existing_schema:
                print "Table definition missing or incorrect in DB, exiting."
                return False

        return True

    def make_schema_object(self):
        existing_schema = self.get_db_schema()  # type: list
        db_tables = {}

        if not existing_schema:
            return False

        for t in existing_schema:
            split_string = t.split('(')  # type: str
            table_name = split_string[0].replace('CREATE TABLE ', '')
            db_tables[table_name] = {'columns': []}
            split_column_string = split_string[1].split(',')
            for c in split_column_string:
                column_split = c.split(' ', 1)
                column = {"name": column_split[0], "type": column_split[1]}
                db_tables[table_name]['columns'].append(column)

        return db_tables

    def drop_db_tables(self):
        drop_tables_script = ""
        for t in self.schema.tables:
            drop_tables_script += "DROP TABLE IF EXISTS %s;" % t
        self.cur.executescript(drop_tables_script)
        self.con.commit()

        return True

    def make_sorted_list_from_dict(self, data, table_name, prefix=''):
        ret_list = []
        for c in self.schema.tables[table_name].columns:
            if c not in self.schema.tables[table_name].index_columns:
                col_label = prefix + c
                if col_label in data:
                    if type(data[col_label]) is str:
                        ret_list.append(data[col_label])
                    elif type(data[col_label]) is list:
                        if type(data[col_label][0]) is str:
                            ret_list.append(data[col_label][0])
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
        return ret_list

    def make_simple_select_command(self, table_name):
        select_command = "SELECT * FROM %s " % table_name
        if table_name not in self.schema.tables:
            return False
        for column_name in self.schema.tables[table_name].columns:
            column_info = self.schema.tables[table_name].columns[column_name]
            if 'FOREIGN KEY' in column_info.type:
                select_command += "INNER JOIN %s ON %s.%s = %s.%s " % \
                    (column_info.foreign_key['table'], table_name, column_name, column_info.foreign_key['table'],
                     column_name)
        return select_command

    def make_complex_select_command(self, table_name, additional_joins):
        select_command = self.make_simple_select_command(table_name)
        if not select_command:
            return False
        for j in additional_joins:
            select_command += "INNER JOIN %s ON %s = %s " % (j['joined_table'], j['left_join'], j['right_join'])

        return select_command

    def make_insert_command(self, table_name):
        insert_command = "INSERT INTO %s (" % table_name
        if table_name not in self.schema.tables:
            return False
        insert_command += self.schema.tables[table_name].get_columns_string(exclude_index=True)
        insert_command += ") VALUES("
        num_columns = self.schema.tables[table_name].get_num_columns(exclude_index=True)
        insert_command += dbmangler_utils.make_list_string_from_char('?', num_columns)
        insert_command += ")"
        return insert_command

    def make_update_command(self, table_name):
        update_command = "UPDATE %s SET " % table_name
        first_entry = True
        if table_name not in self.schema.tables:
            return False
        for column_name in self.schema.tables[table_name].columns:
            if column_name not in self.schema.tables[table_name].index_columns:
                if not first_entry:
                    update_command += ", %s=?" % column_name
                else:
                    update_command += "%s=?" % column_name
                    first_entry = False
        update_command += " WHERE "
        first_entry = True
        for column_name in self.schema.tables[table_name].columns:
            if column_name not in self.schema.tables[table_name].index_columns:
                if not first_entry:
                    update_command += "AND %s=? " % column_name
                else:
                    update_command += "%s=? " % column_name
                    first_entry = False

        return update_command

    def make_delete_command(self, table_name):
        delete_command = "DELETE FROM %s WHERE " % table_name
        first_entry = True
        if table_name not in self.schema.tables:
            return False
        for column_name in self.schema.tables[table_name].columns:
            if column_name not in self.schema.tables[table_name].index_columns:
                if not first_entry:
                    delete_command += "AND %s=? " % column_name
                else:
                    delete_command += "%s=? " % column_name
                    first_entry = False

        return delete_command

    def get_subset_table_rows(self, table_name, conditions, additional_joins=None, order_and_limit=None):

        if not additional_joins:
            select_command = self.make_simple_select_command(table_name)
        else:
            select_command = self.make_complex_select_command(table_name, additional_joins)
        first_item = True
        for c in conditions:
            if first_item:
                first_item = False
                select_command += 'WHERE '
            else:
                select_command += 'AND '
            if type(c[c.keys()[0]]) is list:
                select_command += "%s = '%s' " % (c.keys()[0], c[c.keys()[0]][0])
            elif type(c[c.keys()[0]]) is str or type(c[c.keys()[0]]) is int:
                select_command += "%s = '%s' " % (c.keys()[0], c[c.keys()[0]])

        if order_and_limit:
            if 'order' in order_and_limit.keys():
                first_item = True
                for o in order_and_limit['order']:
                    if first_item:
                        first_item = False
                        select_command += 'ORDER BY '
                    else:
                        select_command += ', '
                    select_command += '%s ' % o
            if 'limit' in order_and_limit.keys():
                select_command += 'LIMIT %s ' % order_and_limit['limit']

        try:
            self.cur.execute(select_command)
        except sqlite3.Error as e:
            print e
            return False

        rows = self.cur.fetchall()

        return rows

    def get_all_table_rows(self, table_name):

        select_command = self.make_simple_select_command(table_name)
        try:
            self.cur.execute(select_command)
        except sqlite3.Error as e:
            print e
            return False

        rows = self.cur.fetchall()

        return rows

    def _check_data(self, table_name, data):

        if table_name not in self.schema.tables:
            return False

        if len(data) < self.schema.tables[table_name].get_num_columns(exclude_index=True):
            print 'ERROR: number of columns and items in data do not match'
            return False

        if type(data) is dict:
            data = self.make_sorted_list_from_dict(data, table_name)
            if not data:
                print 'ERROR: could not make list from data dict'
                return False

        return data

    def add_table_row(self, table_name, data):

        data = self._check_data(table_name, data)

        if not data:
            return False

        insert_command = self.make_insert_command(table_name)
        try:
            self.cur.execute(insert_command, data)
        except sqlite3.Error as e:
            print e
            return False

        self.con.commit()
        return True

    def delete_table_row(self, table_name, data):

        data = self._check_data(table_name, data)

        if not data:
            return False

        delete_command = self.make_delete_command(table_name)
        try:
            self.cur.execute(delete_command, data)
        except sqlite3.Error as e:
            print e
            return False

        self.con.commit()
        return True

    def update_table_row(self, table_name, old_data, new_data):

        new_data_list = self._check_data(table_name, new_data)
        old_data_list = self._check_data(table_name, old_data)
        if not new_data_list or not old_data_list:
            return False
        data = new_data_list+old_data_list  # type: list

        update_command = self.make_update_command(table_name)

        try:
            self.cur.execute(update_command, data)
        except sqlite3.Error as e:
            print e
            return False

        self.con.commit()
        return True

    def get_row_insert_if_not_found(self, table_name, data):

        res = self.get_subset_table_rows(table_name, [data])
        if len(res) == 1:
            return res[0]

        elif len(res) == 0:
            if not self.add_table_row(table_name, data):
                return False
            res = self.get_subset_table_rows(table_name, [data])
            if len(res) == 1:
                return res[0]

        return False

    def get_joined_table_rows(self, table_name):
        joined_tables = {}
        for column_name in self.schema.tables[table_name].columns:
            if self.schema.tables[table_name].columns[column_name].foreign_key:
                fk = self.schema.tables[table_name].columns[column_name].foreign_key
                if fk['table'] not in joined_tables:
                    joined_tables[fk['table']] = self.get_all_table_rows(fk['table'])
        return joined_tables
