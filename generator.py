from yaml import load, YAMLError


def load_data(filename):
    try:
        with open(filename, 'r') as source:
            dump_data = load(source)
            return dump_data
    except FileNotFoundError:
        print("No such file or directory: {}".format(filename))
        return False
    except YAMLError:
        print("Could not parse file: {}".format(filename))
        return False
    finally:
        source.close()


def generate_ddl_statements(filename):
    dump_data = load_data(filename)
    if dump_data:
        result = ''
        for (table_name, properties) in dump_data.items():
            table_name = table_name.lower()
            result = '\n'.join([result, 'CREATE TABLE "{}"('.format(table_name)])
            for (property_name, fields) in properties.items():
                if property_name == 'fields':
                    for (field, value) in fields.items():
                        result = '\n\t'.join([result, '{} {}, '.format(field, value)])
            result = '\n\t'.join([result, '{}_created timestamp NOT NULL DEFAULT now(),'.format(table_name), \
                                  '{}_updated timestamp NOT NULL DEFAULT now()\n);'.format(table_name),])
        print(result)
    else:
        print("Nothing to parse")


if __name__ == '__main__':
    generate_ddl_statements('test.yaml')


