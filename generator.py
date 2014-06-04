from yaml import load, YAMLError


def load_data(filename):
    try:
        with open(filename, 'r') as source:
            dump_data = load(source)
            if dump_data is None:
                print("File is empty. Nothing to parse")
            source.close()
            return dump_data
    except FileNotFoundError:
        print("No such file or directory: {}".format(filename))
        return False
    except YAMLError:
        print("Could not parse file: {}".format(filename))
        source.close()
        return False


def generate_ddl_statements(in_file, out_file):
    dump_data = load_data(in_file)
    if dump_data:
        result = ''
        for (table_name, properties) in dump_data.items():
            table_name = table_name.lower()
            result = '\n'.join([result, 'CREATE TABLE "{}"('.format(table_name)])
            for (property_name, fields) in properties.items():
                if property_name == 'fields':
                    result = '\t'.join([result, '\n\t{}_id SERIAL NOT NULL, '.format(table_name)])
                    for (field, value) in fields.items():
                        result = '\n\t'.join([result, '{}_{} {}, '.format(table_name, field, value)])
            result = '\n\t'.join([result, '{}_created timestamp NOT NULL DEFAULT now(),'.format(table_name), \
                                  '{}_updated timestamp NOT NULL DEFAULT now()\n);'.format(table_name),])
        try:
            f = open(out_file, 'e')
            f.write(result)
            f.close()
        except:
            print('Unable to write to file')

if __name__ == '__main__':
    generate_ddl_statements('test.yaml', 'statements.sql')