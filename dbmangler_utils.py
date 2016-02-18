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
