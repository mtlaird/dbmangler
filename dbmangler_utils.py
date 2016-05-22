import json
import pytoml


def decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = decode_list(item)
        elif isinstance(item, dict):
            item = decode_dict(item)
        rv.append(item)
    return rv


def decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = decode_list(value)
        elif isinstance(value, dict):
            value = decode_dict(value)
        rv[key] = value
    return rv


def make_list_string_from_dict(input_dict, name, skip_values=None):
    ret_string = ""
    first_item = True
    for d in input_dict:
        if d[name] not in skip_values:
            if first_item:
                first_item = False
            else:
                ret_string += ", "
            ret_string += d[name]
    return ret_string


def make_list_string_from_char(input_char, length):
    ret_string = ""
    first_item = True
    for i in range(0, length):
        if first_item:
            first_item = False
        else:
            ret_string += ", "
        ret_string += input_char
    return ret_string


class DB:
    def __init__(self, config_file_name='example_db_config.json'):
        if config_file_name[-5:] == '.toml':
            with open(config_file_name) as data_file:
                db_config = pytoml.load(data_file)
        else:
            with open(config_file_name) as data_file:
                db_config = json.load(data_file, object_hook=decode_dict)
        self.schema = DBSchema(db_config)

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


class DBSchema:

    class DBTable:

        class DBColumn:
            def __init__(self, column_name, column_def):
                self.name = column_name
                if 'label' in column_def:
                    self.label = column_def['label']
                else:
                    self.label = column_name.replace("_", " ").title()
                if type(column_def['type']) is list:
                    self.type = column_def['type']
                elif type(column_def['type']) is str or type(column_def['type']) is unicode:
                    self.type = [column_def['type']]
                if 'FOREIGN KEY' in self.type and 'foreign_key' in column_def:
                    self.foreign_key = column_def['foreign_key']

        class JoinedTable:
            def __init__(self, table_name, table_def):
                self.name = table_name
                # Joined Columns determine which columns are used to join the table
                if 'joined_columns' in table_def:
                    self.joined_columns = table_def['joined_columns']
                # Included Columns determine which columns from the joined table are included in select statements
                if 'included_columns' in table_def:
                    self.included_columns = table_def['included_columns']
                else:
                    self.included_columns = '*'
                # Recursive Join is a boolean which determines whether the joined table should also be joined
                if 'recursive_join' in table_def:
                    self.recursive_join = table_def['recursive_join']
                else:
                    self.recursive_join = True

        def __init__(self, table_name, table_def):
            self.name = table_name
            if 'label' in table_def:
                self.label = table_def['label']
            else:
                self.label = table_name.title()
            self.columns = {}
            for column_name in table_def['columns']:
                column_def = table_def['columns'][column_name]
                self.columns[column_name] = self.DBColumn(column_name, column_def)
            self.index_columns = []
            for column_name in self.columns:
                column_def = self.columns[column_name]
                if 'PRIMARY KEY' in column_def.type:
                    self.index_columns.append(column_name)
            if 'joined_tables' in table_def:
                self.joined_tables = {}
                for joined_table_name in table_def['joined_tables']:
                    joined_table_def = table_def['joined_tables'][joined_table_name]
                    self.joined_tables[joined_table_name] = self.JoinedTable(joined_table_name, joined_table_def)

        def get_columns_string(self, exclude_index=False):
            ret_string = ""
            first_entry = True
            for column_name in self.columns:
                if not exclude_index or column_name not in self.index_columns:
                    if first_entry:
                        ret_string = column_name
                        first_entry = False
                    else:
                        ret_string += ", " + column_name
            return ret_string

        def get_num_columns(self, exclude_index=False):
            num_columns = len(self.columns)
            if exclude_index:
                num_columns -= len(self.index_columns)
            return num_columns

    def __init__(self, schema_def):
        self.tables = {}
        for table_name in schema_def['tables']:
            table_def = schema_def['tables'][table_name]
            self.tables[table_name] = self.DBTable(table_name, table_def)
