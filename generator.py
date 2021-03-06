from yaml import load, YAMLError


class Generator(object):
    """
    Generate DDL statements from YAML format
    """
    __create_table_string = """CREATE TABLE "{table}"(
    {table}_id serial NOT NULL,
    {columns}
    {table}_created timestamp NOT NULL DEFAULT now(),
    {table}_updated timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY ({table}_id)\n);\n\n"""

    __create_join_table = """CREATE TABLE "{left}__{right}" (
    "{left}_id" integer NOT NULL,
    "{right}_id" integer NOT NULL,
    PRIMARY KEY ({left}_id, {right}_id)\n);\n\n"""

    __alter_string = """ALTER TABLE "{child}" ADD "{parent}_id" integer NOT NULL,
    ADD CONSTRAINT "fk_{child}_{parent}_id" FOREIGN KEY ("{parent}_id")
    REFERENCES "{parent}" ("{parent}_id");\n\n"""

    __trigger_proc_head = """CREATE OR REPLACE FUNCTION update_{table}_timestamp()\nRETURNS TRIGGER AS $$\n"""

    __trigger_proc_body = "BEGIN\n\tNEW.{table}_updated = now();\n\tRETURN NEW;\nEND;\n"

    __create_trigger_str = """$$ language 'plpgsql';\nCREATE TRIGGER "tr_{table}_updated"
    BEFORE UPDATE ON "{table}" FOR EACH ROW EXECUTE PROCEDURE update_{table}_timestamp();\n\n"""

    def __init__(self):
        self._alters = set()
        self._tables = set()
        self._triggers = set()
        self._schema = ''

    def __build_tables(self):
        """
        Generate CREATE TABLE  statements
        """
        for entity in self._schema.keys():
            format_params = {'table': entity.lower(), 'columns': '\n\t\t'.join(self.__build_columns(entity))}
            self._tables.add(self.__create_table_string.format(**format_params))

    def __build_columns(self, entity):
        """
        Generate list of columns for entity (table)

        :entity, string - name of the table
        """
        for (field, value) in self._schema[entity]['fields'].items():
            format_params = (entity.lower(), field, value)
            yield '{}_{} {}, '.format(*format_params)

    def __build_relations(self):
        """
        Generate relations between tables (many-to-one or many-to-many)
        """
        def _order_tables(table1, table2):
            return tuple(sorted([table1.lower(), table2.lower()]))

        for entity in self._schema.keys():
            for (table_name, relation_type) in self._schema[entity]['relations'].items():
                try:
                    if relation_type == 'one' and self._schema[table_name]['relations'][entity] == 'many':
                        self.__build_many_to_one(entity.lower(), table_name.lower())
                    if relation_type == 'many' and self._schema[table_name]['relations'][entity] == 'many':
                        self.__build_many_to_many(*_order_tables(entity.lower(), table_name.lower()))
                except KeyError:
                    print('Bad schema. Could not find correct relations between {} '
                          'and {}'.format(table_name, entity))

    def __build_many_to_one(self, child, parent):
        self._alters.add(self.__alter_string.format(child=child, parent=parent))

    def __build_many_to_many(self, left, right):
        self._tables.add(self.__create_join_table.format(left=left, right=right))

    def __build_triggers(self):
        for entity in self._schema.keys():
            trigger_string = ''.join([self.__trigger_proc_head, self.__trigger_proc_body, self.__create_trigger_str])
            self._triggers.add(trigger_string.format(table=entity.lower()))

    def build_ddl(self, filename):
        """
        parse yaml schema and fill tables, alters and triggers
        """
        self._schema = self.__class__.load_data(filename)
        if self._schema:
            self.__build_tables()
            self.__build_triggers()
            self.__build_relations()

    def clear(self):
        """
        clear tables, alters and triggers
        """
        self._alters = set()
        self._tables = set()
        self._triggers = set()
        self._schema = ''

    def dump(self, filename):
        """
        write tables, then alters and triggers to file
        """
        try:
            f = open(filename, 'w')
            f.write(''.join([table for table in self._tables]))
            f.write(''.join([alter for alter in self._alters]))
            f.write(''.join([trigger for trigger in self._triggers]))
            f.close()
        except IOError:
            print('Unable to write to file')

    @staticmethod
    def load_data(filename):
        """
        Load data from file
        :filename, string, name of file
        """
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
    generator.build_ddl('schema.yaml')
    generator.dump('schema.sql')