import psycopg2
import psycopg2.extras
import faker
import csv
import random
from collections import defaultdict
import argparse
from concurrent.futures import ThreadPoolExecutor

fake = faker.Faker()
serials = {}


def serial(name, seed=0):
    if not name in serials:
        serials[name] = seed

    def cap(_):
        serials[name] += 1
        return serials[name]
    return cap


def get_mapper(table_schema, table_name, column_name, data_type, pii_type, **_):
    def tla(*_):
        c = 'QWERTYUIOPLKJHGFDSAMNBVCXZ'
        return ''.join((random.choice(c) for _ in range(5)))

    def slug(_):
        c = 'QWERTYUIOPLKJHGFDSAMNBVCXZ'
        return ''.join((random.choice(c) for _ in range(15)))

    fakers = {
        'person_firstname': lambda _: fake.first_name(),
        'person_familyname': lambda _: fake.last_name(),
        'person_name': lambda _: fake.name(),
        'tla': tla,
        'business_name': lambda _: fake.company(),
        'slug': lambda _: fake.slug(),
        'null': lambda _: None,
        'text_short': lambda _: fake.sentence(),
        'text': lambda _: fake.paragraph(),
        'email': lambda _: fake.email(domain='example.com'),
        'user_agent': lambda _: fake.user_agent(),
        'url': lambda _: fake.uri(),
        'url_image': lambda _: fake.image_url(),
        'phonenumber': lambda _: fake.msisdn()[:11],
        'address': lambda _: fake.address(),
        'city': lambda _: fake.city(),
        'zipcode': lambda _: fake.postcode(),
        'filename': lambda _: fake.file_name(),
        'inet_addr': lambda _: fake.ipv4(),
        'username': slug,
        'int': lambda _: random.randint(0, 300000000),
        'password': slug,
        'serial': serial(f"{table_schema}.{table_name}.{column_name}", 200000000),

    }
    return fakers[pii_type]


def get_piis(source_file_csv: str):
    source = csv.DictReader(open(source_file_csv), delimiter=',')
    tables: dict = defaultdict(dict)
    for line in source:
        if line['pii'] == 'yes':
            try:
                mapper = get_mapper(**line)

                tables[f"{line['table_schema']}.{line['table_name']}"][line['column_name']] = (
                    line, mapper)
            except:
                print(line)
                raise
    return tables


def mask_row(row, mappers):
    new_row = {}
    for column_name, (line, mapper) in mappers.items():
        new_row[line['column_name']] = mapper(line)
    return new_row


def mask_pii(table, mappers, dsn):
    print(f'Executing {table}')
    conn = psycopg2.connect(dsn)
    read_cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    read_cursor.execute(f"""SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
                            FROM   pg_index i
                            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                                 AND a.attnum = ANY(i.indkey)
                            WHERE  i.indrelid = '{table}'::regclass
                            AND    i.indisprimary;""")
    pks = [row[0] for row in read_cursor]
    read_cursor.execute(f"SELECT * FROM {table}")
    write_cursor = conn.cursor()
    for row in read_cursor:
        new_row = mask_row(row, mappers)
        where = ' AND '.join((f"{colname}=%s") for colname in pks)
        replacements = ','.join((f"{colname}=%s")
                                for colname in new_row.keys())
        new_values = [new_row[k] for k in new_row.keys()]
        old_values = [row[k] for k in pks]
        sql = f"UPDATE {table} SET {replacements} WHERE {where}"
        values = new_values+old_values
        try:
            write_cursor.execute(sql, values)
            assert write_cursor.rowcount == 1
        except Exception as e:
            print(table)
            print(e)
            print('row', row)
            print(old_values)
            print(new_values)
            print('new', new_row)
            raise
    conn.commit()
    print(f'{table} commited')

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dburl', type=str,)
    parser.add_argument('-f', '--filename', type=str,)
    parser.add_argument('-e', '--executors', type=int, default=2)
    args = parser.parse_args()
    executor = ThreadPoolExecutor(max_workers=args.executors)
    tasks = []
    for table, mappers in get_piis(args.filename).items():
        print(table)
        if mappers:
            print(f"  masking {', '.join(mappers.keys())}")
            tasks.append(executor.submit(mask_pii, table, mappers, args.dburl))
        else:
            print('  not masking')

    
    completed = 0
    taskcount = len(tasks)
    print(f"{taskcount} tasks submitted")
    for task in tasks:
        task.result()
        completed += 1
        print(f"{completed}/{taskcount}")
    print('Done')


if __name__ == "__main__":
    main()
