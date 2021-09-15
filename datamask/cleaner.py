import argparse
import csv
import json
import logging
import random
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from threading import Lock

import faker
from slugify import slugify

logging.basicConfig()
LOG = logging.getLogger(__name__)
FAKER = faker.Faker()
SERIALS = {}
SERIAL_LOCK = Lock()


def tla(*_):
    """Create a random abbreviation-looking string"""
    c = "QWERTYUIOPLKJHGFDSAMNBVCXZ"
    return "".join(random.choice(c) for _ in range(5))


def serial(name, seed=0):
    """Create a serial"""
    if name not in SERIALS:
        SERIALS[name] = seed

    def cap(*_):
        with SERIAL_LOCK:
            SERIALS[name] += 1
            return SERIALS[name]

    return cap


def random_slug(*_):
    """Create a random slug"""
    c = "QWERTYUIOPLKJHGFDSAMNBVCXZ"
    return "".join(random.choice(c) for _ in range(15))


def slug(args):

    if not args or not args[0]:
        return random_slug

    def slug_with_args(field, row):
        """Create a slug from another named column on the same row"""
        return slugify(row[args[0]])

    return slug_with_args


def generic(fn, doc=None):
    """Helper for generic faker functions that take no arguments"""

    def fake_maker(*args):
        @wraps(fn)
        def faker(field, row):
            return fn()

        return faker

    if doc is None:
        fake_maker.__doc__ = fn.__doc__
    else:
        fake_maker.__doc__ = doc
    return fake_maker


FieldMapSpec = namedtuple("FieldMapSpec", "col spec mapper")


class RowMapper:
    def __init__(self, piis_for_table):
        """Sort column mappers according to dependencies"""
        self.mappers = []
        remaining = dict(piis_for_table.items())
        added = set()
        count = len(remaining)
        while remaining:
            for col, (spec, mapper) in remaining.items():
                if spec["depends"]:
                    depends = [d for d in spec["depends"].split(",") if d]
                else:
                    depends = None
                if not depends or all([dep in added for dep in depends]):
                    self.mappers.append(FieldMapSpec(col, spec, mapper))
                    added.add(col)
            for v in added:
                del remaining[v]
            assert len(remaining) < count
            count = len(remaining)

    def mask(self, row):
        for mapspec in self.mappers:
            try:
                row[mapspec.col] = mapspec.mapper(row[mapspec.col], row)
            except Exception:
                print(f"Failed at col {mapspec.col} using mapper {mapspec.mapper}")
                raise
        return row


FAKERS = {
    "person_firstname": generic(FAKER.first_name, "A first name"),
    "person_familyname": generic(FAKER.last_name, " A family name"),
    "person_name": generic(FAKER.name),
    "tla": generic(tla),
    "business_name": generic(FAKER.company),
    "slug": slug,
    "null": generic(lambda: None, "Returns NULL"),
    "text_short": generic(FAKER.sentence),
    "text": generic(FAKER.paragraph),
    "email": generic(
        lambda *_: FAKER.email(domain="example.com"),
        doc="Returns an @example.com email address",
    ),
    "user_agent": generic(FAKER.user_agent),
    "url": generic(FAKER.uri),
    "url_image": generic(FAKER.image_url),
    "phonenumber": generic(FAKER.msisdn),
    "address": generic(FAKER.address),
    "city": generic(FAKER.city),
    "zipcode": generic(FAKER.postcode),
    "filename": generic(FAKER.file_name),
    "inet_addr": generic(FAKER.ipv4),
    "username": slug,
    "int": lambda _: random.randint(0, 300000000),  # TODO: arg+default
    "password": slug,
    # TODO: arg+default instead?
    "serial": generic(lambda: None, doc="Imitate a serial"),
}


def get_mapper(
    table_schema, table_name, column_name, data_type, pii_type, args, depends, **_
):
    if pii_type == "serial":
        return serial(f"{table_schema}.{table_name}.{column_name}", 200000000)
    else:
        return FAKERS[pii_type](args.split(","))


def get_piis(source_csv):
    source = csv.DictReader(source_csv, delimiter=";")
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


def mask_pii(table: str, pii_spec, dsn, keepers, fixed):

    row_mapper = RowMapper(pii_spec)
    if not row_mapper.mappers:
        print(f"Skipping {table}")
        return
    print(f"Executing {table}")
    if dsn.startswith("postgres"):
        import psycopg2
        import psycopg2.extras

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
        write_cursor = conn.cursor()
        write_cursor.execute("SET CONSTRAINTS ALL DEFERRED")
        p = "%s"
    else:
        import sqlite3

        conn = sqlite3.connect(dsn)
        conn.row_factory = sqlite3.Row
        read_cursor = conn.cursor()
        read_cursor.execute(
            "SELECT name from pragma_table_info(?) WHERE pk=1", (table.split(".")[1],)
        )
        write_cursor = conn.cursor()
        p = "?"
    pks = [row[0] for row in read_cursor]
    if keepers is None:
        read_cursor.execute(f"SELECT * FROM {table}")
    else:
        print(f"Skipping {len(keepers)} records for {table}")
        read_cursor.execute(
            f"SELECT * FROM {table} WHERE {pks[0]} NOT IN ({','.join([p]*len(keepers))})",
            keepers,
        )
    where = " AND ".join((f"{colname}={p}") for colname in pks)
    for row in read_cursor:
        try:

            new_row = row_mapper.mask({k: row[k] for k in row.keys()})
        except Exception as exe:
            print(f"{table} Failure ({exe}):\n\t{row}\n\t{pii_spec}")
            raise
        replacements = ",".join((f'"{colname}"={p}') for colname in new_row.keys())
        new_values = [new_row[k] for k in new_row.keys()]
        old_values = [row[k] for k in pks]
        sql = f"UPDATE {table} SET {replacements} WHERE {where}"
        values = new_values + old_values
        try:
            write_cursor.execute(sql, values)
            assert write_cursor.rowcount == 1
        except Exception:
            LOG.exception(f"Table: {table}\n SQL: {sql}")
            raise
    if fixed:
        for pk, kv in fixed.items():
            update_sql = []
            vals = []
            for col, val in kv.items():
                update_sql.append(f"{col}={p}")
                vals.append(val)
            write_cursor.execute(
                f"UPDATE {table} SET {','.join(update_sql)} WHERE {where}", vals + [pk]
            )
    conn.commit()
    LOG.info(f"{table} commited")


def clean(
    executors: int, filename: str, dburl: str, keep_filename: str, fixed_filename: str
):
    if not keep_filename:
        keep = {}
    else:
        keep = json.load(open(keep_filename))
    if not fixed_filename:
        fixed = {}
    else:
        fixed = json.load(open(fixed_filename))
    executor = ThreadPoolExecutor(max_workers=executors)
    tasks = []
    source_csv = open(filename)
    for table, pii_spec in get_piis(source_csv).items():

        tasks.append(
            executor.submit(
                mask_pii,
                table,
                pii_spec,
                dburl,
                keep.get(table, None),
                fixed.get(table, None),
            )
        )

    completed = 0
    taskcount = len(tasks)
    LOG.debug(f"{taskcount} tasks submitted")
    for task in tasks:
        task.result()
        completed += 1
        LOG.debug(f"{completed}/{taskcount}")
    LOG.info("Done")


def print_fakers():
    print(
        "Available pii_types / fakes: \n"
        + "\n".join(
            [
                f"  {key}: {FAKERS[key].__doc__ or 'No doc =('}"
                for key in [key for key in sorted(FAKERS.keys())]
            ]
        )
    )


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--list-fakers", action="store_true", default=False)
    parser.add_argument(
        "-d",
        "--dburl",
        type=str,
    )
    parser.add_argument(
        "-f",
        "--filename",
        type=str,
    )
    parser.add_argument(
        "--keep",
        type=str,
        help='JSON file mapping rows to not mask {"schema.table": ["pk1", "pk2"]}',
        default=None,
    )
    parser.add_argument(
        "--fixed",
        type=str,
        help='JSON file mapping fixed masks: {"schema.table": {"pk1": {"col": "val"}]}',
        default=None,
    )
    parser.add_argument("-e", "--executors", type=int, default=2)
    args = parser.parse_args()
    if args.list_fakers:
        print_fakers()
    else:
        clean(
            executors=args.executors,
            filename=args.filename,
            dburl=args.dburl,
            keep_filename=args.keep,
            fixed_filename=args.fixed,
        )


if __name__ == "__main__":
    main()
