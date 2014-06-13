from yaml import load, YAMLError


class Generator(object):
    __create_table_string = """CREATE TABLE "{table}"(
    {table}_id serial NOT NULL,
    {columns}
    {table}_created timestamp NOT NULL DEFAULT now(),
    {table}_updated timestamp NOT NULL DEFAULT now()
);\n"""

    __trigger_string = """\nCREATE OR REPLACE FUNCTION update_{table}_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.{table}_updated = now();
    RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER "tr_{table}_updated" BEFORE UPDATE ON "table" FOR EACH ROW EXECUTE\n\
PROCEDURE update_{table}_timestamp();\n"""

    __create_join_table = """CREATE TABLE "{left}__{right}" (
    "{left}_id" integer NOT NULL,
    "{right}_id" integer NOT NULL,
    PRIMARY KEY ("{left}_id", "{right}_id")
);\n"""
    __alter_string = """\nALTER TABLE "{child}" ADD "{parent}_id" integer NOT NULL,
    ADD CONSTRAINT "fk_{child}_{parent}_id" FOREIGN KEY ("{parent}_id")
    REFERENCES "{parent}" ("{parent}_id");\n"""


    def __init__(self):
        self._alters   = set()
        self._tables   = set()
        self._triggers = set()
        self._schema = ''

    def __build_tables(self):
        for (entity) in self._schema.keys():
            self._tables.add(self.__create_table_string.format(table=entity.lower(), columns=self.__build_columns(entity)))

    def __build_columns(self, entity):
        for (field, value) in self._schema[entity]['fields'].items():
            field_statement = '\n\t'.join(['{}_{} {}, '.format(entity.lower(), field, value)])
        return field_statement

    def __build_relations(self):
        def _order_tables(table1, table2):
            string1 = table1.lower()
            string2 = table2.lower()
            if string1[0] < string2[0]:
                return {'left': string1, 'right': string2}
            return {'left': string2, 'right': string1}

        for entity in self._schema.keys():
            print(self._schema[entity]['relations'].items())
            for (table_name, relation_type) in self._schema[entity]['relations'].items():
                if relation_type == 'one':
                    self.__build_many_to_one(entity.lower(), table_name.lower())
                if relation_type == 'many':
                    self.__build_many_to_many(**_order_tables(entity.lower(), table_name.lower()))

    def __build_many_to_one(self, child, parent):
        self._alters.add(self.__alter_string.format(child=child, parent=parent))

    def __build_many_to_many(self, left, right):
        self._tables.add(self.__create_join_table.format(left=left, right=right))

    def __build_triggers(self):
        for entity in self._schema.keys():
            self._triggers.add(self.__trigger_string.format(table=entity.lower()))

    def build_ddl(self, filename):
        #parse yaml schema and fill tables, alters and triggers
        self._schema = self.__class__.load_data(filename)
        if self._schema:
            self.__build_tables()
            self.__build_triggers()
            self.__build_relations()

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
            for alter in self._alters:
                f.write(str(alter))
            for trigger in self._triggers:
                f.write(str(trigger))

            f.close()
        except IOError:
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
    generator.build_ddl('schema.yaml')
    generator.dump('schema.sql')
