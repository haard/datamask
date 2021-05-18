import argparse
import csv
import logging
import random
import textwrap
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import faker
import psycopg2
import psycopg2.extras

logging.basicConfig()
LOG = logging.getLogger(__name__)
FAKER = faker.Faker()
SERIALS = {}
SERIAL_LOCK = Lock()


def tla(*_):
    c = "QWERTYUIOPLKJHGFDSAMNBVCXZ"
    return "".join((random.choice(c) for _ in range(5)))


def slug(_):
    c = "QWERTYUIOPLKJHGFDSAMNBVCXZ"
    return "".join((random.choice(c) for _ in range(15)))


def serial(name, seed=0):
    if name not in SERIALS:
        SERIALS[name] = seed

    def cap(_):
        with SERIAL_LOCK:
            SERIALS[name] += 1
            return SERIALS[name]

    return cap


# Somehow still TODO: args and context
FAKERS = {
    "person_firstname": lambda _: FAKER.first_name(),
    "person_familyname": lambda _: FAKER.last_name(),
    "person_name": lambda _: FAKER.name(),
    "tla": tla,
    "business_name": lambda _: FAKER.company(),
    "slug": lambda _: FAKER.slug(),
    "null": lambda _: None,
    # TODO: args for bs etc? maxlen!
    "text_short": lambda _: FAKER.sentence(),
    "text": lambda _: FAKER.paragraph(),
    "email": lambda _: FAKER.email(domain="example.com"),  # TODO
    "user_agent": lambda _: FAKER.user_agent(),
    "url": lambda _: FAKER.uri(),
    "url_image": lambda _: FAKER.image_url(),
    "phonenumber": lambda _: FAKER.msisdn()[:11],
    "address": lambda _: FAKER.address(),
    "city": lambda _: FAKER.city(),
    "zipcode": lambda _: FAKER.postcode(),
    "filename": lambda _: FAKER.file_name(),
    "inet_addr": lambda _: FAKER.ipv4(),  # TODO private?
    "username": slug,
    "int": lambda _: random.randint(0, 300000000),  # TODO: arg+default
    "password": slug,
    # TODO: arg+default instead?
    "serial": None,
}


def get_mapper(table_schema, table_name, column_name, data_type, pii_type, **_):
    if pii_type == 'serial':
        return serial(f"{table_schema}.{table_name}.{column_name}", 200000000)
    else:
        return FAKERS[pii_type]


def get_piis(source_file_csv: str):
    source = csv.DictReader(open(source_file_csv), delimiter=";")  # TODO: args
    tables: dict = defaultdict(dict)
    for line in source:
        if line["pii"] == "yes":
            try:
                mapper = get_mapper(**line)
                tables[f"{line['table_schema']}.{line['table_name']}"][
                    line["column_name"]
                ] = (line, mapper)
            except:
                LOG.exception(f"Erroring on '{line}'")
                raise
    return tables


def mask_row(row, mappers):
    new_row = {}
    for column_name, (line, mapper) in mappers.items():
        try:
            new_row[line["column_name"]] = mapper(line)
        except:
            print('Failed on {line} with: {mapper(line)}')
            raise
    return new_row


def mask_pii(table, mappers, dsn):
    print(f"Executing {table}")
    conn = psycopg2.connect(dsn)
    read_cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    read_cursor.execute(
        f"""SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
                            FROM   pg_index i
                            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                                 AND a.attnum = ANY(i.indkey)
                            WHERE  i.indrelid = '{table}'::regclass
                            AND    i.indisprimary;"""
    )
    pks = [row[0] for row in read_cursor]
    read_cursor.execute(f"SELECT * FROM {table}")
    write_cursor = conn.cursor()
    write_cursor.execute("SET CONSTRAINTS ALL DEFERRED")
    for row in read_cursor:
        new_row = mask_row(row, mappers)
        where = " AND ".join((f"{colname}=%s") for colname in pks)
        replacements = ",".join((f"{colname}=%s") for colname in new_row.keys())
        new_values = [new_row[k] for k in new_row.keys()]
        old_values = [row[k] for k in pks]
        sql = f"UPDATE {table} SET {replacements} WHERE {where}"
        values = new_values + old_values
        try:
            write_cursor.execute(sql, values)
            assert write_cursor.rowcount == 1
        except Exception as e:
            LOG.exception(f"Table: {table}")
            raise
    conn.commit()
    LOG.info(f"{table} commited")


def clean(executors: int, filename: str, dburl: str):

    executor = ThreadPoolExecutor(max_workers=executors)
    tasks = []
    for table, mappers in get_piis(filename).items():
        if mappers:
            LOG.info(f"{table}:  masking {', '.join(mappers.keys())}")
            tasks.append(executor.submit(mask_pii, table, mappers, dburl))
        else:
            LOG.debug(f"{table}: not masking")

    completed = 0
    taskcount = len(tasks)
    LOG.debug(f"{taskcount} tasks submitted")
    for task in tasks:
        task.result()
        completed += 1
        LOG.debug(f"{completed}/{taskcount}")
    LOG.info("Done")


def main():

    parser = argparse.ArgumentParser(
        epilog=f"Available pii_types / fakes: {', '.join((key for key in sorted(FAKERS.keys())))}"
    )

    parser.add_argument(
        "-d", "--dburl", type=str,
    )
    parser.add_argument(
        "-f", "--filename", type=str,
    )
    parser.add_argument("-e", "--executors", type=int, default=2)
    args = parser.parse_args()
    clean(executors=args.executors, filename=args.filename, dburl=args.dburl)

if __name__ == "__main__":
    main()
