from yaml import load, YAMLError


class Generator(object):
    __create_table_string = 'CREATE TABLE "{}"('
    __serial_string       = '\n\t{}_id SERIAL NOT NULL, '
    __created_string      = '{}_created timestamp NOT NULL DEFAULT now(),'
    __updated_string      = '{}_updated timestamp NOT NULL DEFAULT now()'



    def __init__(self):
        self._alters   = set()
        self._tables   = set()
        self._triggers = set()
        self._schema = ''

    def __build_tables(self):
        for (entity) in self._schema.keys():
            table_statement= self.__create_table_string.format(entity.lower())
            self._tables.add('{} {});\n'.format(table_statement, self.__build_columns(entity)))

    def __build_columns(self, entity):
        table_name = entity.lower()
        field_statement = self.__serial_string.format(table_name)
        for (field, value) in self._schema[entity]['fields'].items():
            field_statement = '\n\t'.join([field_statement, '{}_{} {}, '
                .format(table_name, field, value.upper())])
        field_statement = '\n\t'.join([field_statement, self.__created_string.
            format(table_name),self.__updated_string.format(table_name),])
        return field_statement

    def __build_relations(self):
        pass

    def __build_many_to_one(self, child, parent):
        pass

    def __build_many_to_many(self, left, right):
        pass

    def __build_triggers(self):
        pass

    def build_ddl(self, filename):
        #parse yaml schema and fill tables, alters and triggers
        self._schema = self.__class__.load_data(filename)
        if self._schema:
            self.__build_tables()

    def clear(self):
        #clear tables, alters and triggers
        self._alters    = set()
        self._tables    = set()
        self._triggers  = set()
        self._schema = ''


    def dump(self, filename):
        #write tables, then alters and triggers to file
        try:
            f = open(filename, 'w')
            for table in self._tables:
                f.write(str(table))
            f.close()
        except:
            print('Unable to write to file')

    @staticmethod
    def load_data(filename):
        try:
            with open(filename, 'r') as source:
                schema = load(source)
                if schema is None:
                    print("File is empty. Nothing to parse")
                source.close()
                return schema
        except FileNotFoundError:
            print("No such file or directory: {}".format(filename))
            return False
        except YAMLError:
            print("Could not parse file: {}".format(filename))
            return False

if __name__ == '__main__':
    generator = Generator()
    generator.build_ddl('test.yaml')
    generator.dump('statements.sql')
