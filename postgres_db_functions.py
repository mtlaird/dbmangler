import dbmangler_utils
import psycopg2.extras
import psycopg2.errorcodes
import json
import pytoml


def create_db(db_name, db_user, db_user_password=None, postgres_user='postgres', postgres_password=None):
    if not postgres_password:
        postgres_password = raw_input('Enter the postgres user password: ')
    con = psycopg2.connect(user=postgres_user, password=postgres_password)
    con.autocommit = True
    cur = con.cursor()
    if not db_user_password:
        db_user_password = raw_input('Enter password for the DB user: ')
    cur.execute('create user %s with superuser' % db_user)
    cur.execute("alter user %s with password '%s'" % (db_user, db_user_password))
    cur.execute('create database %s with owner = %s' % (db_name, db_user))
    print 'Database and user created successfully.'
    return True


class DB:
    def __init__(self, config_file_name='example_db_config.json'):
        if config_file_name[-5:] == '.toml':
            with open(config_file_name) as data_file:
                db_config = pytoml.load(data_file)
        else:
            with open(config_file_name) as data_file:
                db_config = json.load(data_file, object_hook=dbmangler_utils.decode_dict)
        self.schema = dbmangler_utils.DBSchema(db_config)
        self.db_name = 'testdb'
        self.db_user = 'testuser'
        self.db_password = 'password'
        self.conn_string = "dbname = '%s' user='%s' password='%s'" % (self.db_name, self.db_user, self.db_password)
        self.db_state = 'OK'
        try:
            self.con = psycopg2.connect(self.conn_string)
        except psycopg2.OperationalError:
            self.db_state = 'Faulted'
        if self.db_state != 'Faulted':
            self.cur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def reset_cursor(self):
        self.cur.close()
        self.cur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def run_select_command(self, command, values=None, return_list=False):
        if not values:
            try:
                self.cur.execute(command)
            except Exception as e:
                print e
                return False
        else:
            try:
                self.cur.execute(command, values)
            except Exception as e:
                print e
                return False

        if return_list:
            return self.cur.fetchall()

        res = []
        for record in self.cur:
            res.append(dict(record))

        return res

    def run_edit_command(self, command, values=None):
        try:
            self.cur.execute(command, values)
        except Exception as e:
            print e
            return False

        self.con.commit()

        return True

    def get_db_schema(self):

        res = self.run_select_command("SELECT table_name, column_name, data_type FROM information_schema.columns "
                                      "WHERE table_schema = 'public' AND table_catalog = %s", (self.db_name,))
        if not res:
            return False

        return res

    def create_schema(self, return_script=False, return_array=False):
        create_tables_array = []
        alter_tables_array = []

        # for t in self.db_tables:
        for table_name in self.schema.tables:
            create_tables_script_line = "CREATE TABLE %s(" % table_name
            first_column = True
            foreign_key_in_table = []
            primary_key_in_table = []
            alter_tables_script_line = None
            for column_name in self.schema.tables[table_name].columns:
                column_info = self.schema.tables[table_name].columns[column_name]
                if not first_column:
                    create_tables_script_line += ", "
                else:
                    first_column = False
                temp_column_info_type = list(column_info.type)
                if 'PRIMARY KEY' in column_info.type:
                    primary_key_in_table.append(column_name)
                if 'FOREIGN KEY' in column_info.type:
                    temp_column_info_type.remove('FOREIGN KEY')
                    foreign_key_in_table.append(column_name)
                if 'TEXT' in column_info.type:
                    temp_column_info_type.remove('TEXT')
                    temp_column_info_type.append('character varying')
                if 'AUTOINCREMENT' in column_info.type:
                    temp_column_info_type = ['serial', 'NOT NULL']
                column_type_string = " ".join(temp_column_info_type)
                create_tables_script_line += "%s %s" % (column_name, column_type_string)
            if primary_key_in_table:
                for column_name in primary_key_in_table:
                    create_tables_script_line += ", CONSTRAINT %s_pkey PRIMARY KEY (%s)" % (table_name, column_name)
            if foreign_key_in_table:
                for column_name in foreign_key_in_table:
                    column_info = self.schema.tables[table_name].columns[column_name]
                    alter_tables_script_line = "ALTER TABLE %s ADD CONSTRAINT %s_%s_fkey FOREIGN KEY (%s) " \
                                               "REFERENCES %s (%s) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION" % \
                                               (table_name, table_name, column_name, column_name,
                                                column_info.foreign_key['table'], column_name)

            create_tables_array.append(create_tables_script_line)
            if alter_tables_script_line:
                alter_tables_array.append(alter_tables_script_line)

        create_tables_script = "); ".join(create_tables_array)
        if alter_tables_array:
            alter_tables_script = "; ".join(alter_tables_array)
            create_tables_script += "); " + alter_tables_script + "; "

        if not return_script and not return_array:
            if not self.run_edit_command(create_tables_script):
                self.con.rollback()
                return False

            return True
        elif return_script:
            return create_tables_script
        elif return_array:
            return create_tables_array

    def check_schema(self):
        pass

    def make_schema_object(self):
        pass

    def drop_db_tables(self):
        drop_tables_script = ""
        for t in self.schema.tables:
            drop_tables_script += "DROP TABLE IF EXISTS %s; " % t
        if not self.run_edit_command(drop_tables_script):
            self.con.rollback()
            return False

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
                                  (column_info.foreign_key['table'],
                                   table_name, column_name, column_info.foreign_key['table'], column_name)
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
        return insert_command.replace('?', '%s')

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

        return update_command.replace('?', '%s')

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

        return delete_command.replace('?', '%s')

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

        return self.run_select_command(select_command)

    def get_all_table_rows(self, table_name):

        select_command = self.make_simple_select_command(table_name)

        return self.run_select_command(select_command)

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
        if not self.run_edit_command(insert_command, data):
            return False

        return True

    def delete_table_row(self, table_name, data):

        data = self._check_data(table_name, data)

        if not data:
            return False

        delete_command = self.make_delete_command(table_name)
        if not self.run_edit_command(delete_command, data):
            return False

        return True

    def update_table_row(self, table_name, old_data, new_data):

        new_data_list = self._check_data(table_name, new_data)
        old_data_list = self._check_data(table_name, old_data)
        if not new_data_list or not old_data_list:
            return False
        data = new_data_list+old_data_list  # type: list

        update_command = self.make_update_command(table_name)

        if not self.run_edit_command(update_command, data):
            return False

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
