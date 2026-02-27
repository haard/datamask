import argparse
import csv
import json
import logging
import sys

import yaml
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig()
LOG = logging.getLogger(__name__)


FieldMapSpec = namedtuple("FieldMapSpec", "col spec mapper")


class RowMapper:
    def __init__(self, table, piis_for_table):
        """Sort column mappers according to dependencies (including native fakers)"""
        self.mappers = []
        self.natives = []
        remaining = dict(piis_for_table.items())
        added = set()
        count = len(remaining)
        while remaining:
            for col, (spec, mapper) in list(remaining.items()):
                if isinstance(mapper, NativeFaker):
                    # Check if this native has dependencies
                    native_depends = getattr(mapper, "depends", None)
                    if native_depends and native_depends not in added:
                        continue  # Wait for dependency to be added first
                    self.natives.append(mapper)
                    added.add(col)
                    del remaining[col]
                    continue
                if spec["depends"]:
                    depends = [d for d in spec["depends"].split(",") if d]
                else:
                    depends = None
                if not depends or all([dep in added for dep in depends]):
                    self.mappers.append(FieldMapSpec(col, spec, mapper))
                    added.add(col)
                    del remaining[col]
            assert len(remaining) < count, f"Circular dependency detected: {remaining.keys()}"
            count = len(remaining)

    def mask(self, row):
        for mapspec in self.mappers:
            try:
                data = mapspec.mapper(row[mapspec.col], row)
                row[mapspec.col] = data
            except Exception as exe:
                print(
                    f"Failed at col {mapspec.col} using mapper {mapspec.mapper} with {exe}"
                )
                raise
        for i, col in enumerate(row):
            if isinstance(col, (list, dict)):
                row[i] = json.dumps(row[i])

        return row


class NativeFaker:
    def __init__(self, schema, table, column, args):
        self.schema = schema
        self.table = table
        self.column = column
        self.args = args

    def __str__(self):
        return f"{self.name} for {self.table}:{self.column}"


class EmailFaker(NativeFaker):

    name = "Native email"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(CAST({self.args[0]} AS TEXT), 'email@example.com')  {where}
        """


class StaticStringFaker(NativeFaker):

    name = "Native static string"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}='{self.args[0]}' {where}
        """


class NullFaker(NativeFaker):

    name = "Native null"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=NULL {where}
        """


class IntFaker(NativeFaker):

    name = "Native int"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=FLOOR(RANDOM() * 300000000)::INT {where}
        """


class TlaFaker(NativeFaker):

    name = "Native tla"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=UPPER(SUBSTRING(MD5(RANDOM()::TEXT), 1, 5)) {where}
        """


class ZipcodeFaker(NativeFaker):

    name = "Native zipcode"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=LPAD(FLOOR(RANDOM() * 99999)::TEXT, 5, '0') {where}
        """


class InetAddrFaker(NativeFaker):

    name = "Native inet_addr"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(
            FLOOR(RANDOM() * 256)::INT, '.',
            FLOOR(RANDOM() * 256)::INT, '.',
            FLOOR(RANDOM() * 256)::INT, '.',
            FLOOR(RANDOM() * 256)::INT
        ) {where}
        """


class PhoneNumberFaker(NativeFaker):

    name = "Native phonenumber"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=LPAD(FLOOR(RANDOM() * 999999999999999)::TEXT, 15, '0') {where}
        """


# Data arrays for array-based fakers
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Emma", "Oliver", "Ava", "Noah", "Sophia",
    "Liam", "Isabella", "Mason", "Mia", "Lucas", "Charlotte", "Ethan", "Amelia",
]

FAMILY_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
]

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "Seattle", "Denver", "Boston", "Portland",
    "Las Vegas", "Detroit", "Memphis", "Baltimore", "Milwaukee", "Albuquerque",
]

STREET_NAMES = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd", "Elm St", "Washington Blvd",
    "Park Ave", "Lake Dr", "Hill Rd", "River Rd", "Forest Ave", "Sunset Blvd", "Broadway",
    "Highland Ave", "Valley Rd", "Spring St", "Church St", "Mill Rd", "School St",
]

BUSINESS_SUFFIXES = [
    "Inc", "LLC", "Corp", "Co", "Ltd", "Group", "Solutions", "Services", "Industries",
    "Enterprises", "Holdings", "Partners", "Associates", "International", "Systems",
]

BUSINESS_WORDS = [
    "Acme", "Global", "Premier", "Advanced", "Dynamic", "Innovative", "Strategic",
    "United", "Pacific", "Atlantic", "Summit", "Pioneer", "Apex", "Prime", "Elite",
    "Core", "Next", "Future", "Smart", "Tech", "Digital", "Data", "Cloud", "Net",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (iPad; CPU OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Android 11; Mobile) AppleWebKit/537.36",
]

FILE_EXTENSIONS = ["pdf", "doc", "txt", "jpg", "png", "xlsx", "csv", "mp3", "mp4", "zip"]

LOREM_WORDS = [
    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
    "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore",
    "magna", "aliqua", "enim", "ad", "minim", "veniam", "quis", "nostrud",
    "exercitation", "ullamco", "laboris", "nisi", "aliquip", "ex", "ea", "commodo",
    "consequat", "duis", "aute", "irure", "in", "reprehenderit", "voluptate",
    "velit", "esse", "cillum", "fugiat", "nulla", "pariatur", "excepteur", "sint",
    "occaecat", "cupidatat", "non", "proident", "sunt", "culpa", "qui", "officia",
    "deserunt", "mollit", "anim", "id", "est", "laborum",
]


def _sql_array(items):
    """Convert Python list to PostgreSQL ARRAY literal."""
    escaped = [item.replace("'", "''") for item in items]
    return "ARRAY[" + ",".join(f"'{item}'" for item in escaped) + "]"


class ArrayFaker(NativeFaker):
    """Base class for fakers that pick random items from an array."""

    array = []

    def command(self, where):
        arr = _sql_array(self.array)
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=({arr})[1 + FLOOR(RANDOM() * {len(self.array)})::INT] {where}
        """


class PersonFirstNameFaker(ArrayFaker):
    name = "Native person_firstname"
    array = FIRST_NAMES


class PersonFamilyNameFaker(ArrayFaker):
    name = "Native person_familyname"
    array = FAMILY_NAMES


class PersonNameFaker(NativeFaker):
    name = "Native person_name"

    def command(self, where):
        first_arr = _sql_array(FIRST_NAMES)
        last_arr = _sql_array(FAMILY_NAMES)
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(
            ({first_arr})[1 + FLOOR(RANDOM() * {len(FIRST_NAMES)})::INT],
            ' ',
            ({last_arr})[1 + FLOOR(RANDOM() * {len(FAMILY_NAMES)})::INT]
        ) {where}
        """


class CityFaker(ArrayFaker):
    name = "Native city"
    array = CITIES


class AddressFaker(NativeFaker):
    name = "Native address"

    def command(self, where):
        street_arr = _sql_array(STREET_NAMES)
        city_arr = _sql_array(CITIES)
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(
            FLOOR(RANDOM() * 9999 + 1)::INT, ' ',
            ({street_arr})[1 + FLOOR(RANDOM() * {len(STREET_NAMES)})::INT], ', ',
            ({city_arr})[1 + FLOOR(RANDOM() * {len(CITIES)})::INT], ' ',
            LPAD(FLOOR(RANDOM() * 99999)::TEXT, 5, '0')
        ) {where}
        """


class BusinessNameFaker(NativeFaker):
    name = "Native business_name"

    def command(self, where):
        word_arr = _sql_array(BUSINESS_WORDS)
        suffix_arr = _sql_array(BUSINESS_SUFFIXES)
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(
            ({word_arr})[1 + FLOOR(RANDOM() * {len(BUSINESS_WORDS)})::INT], ' ',
            ({suffix_arr})[1 + FLOOR(RANDOM() * {len(BUSINESS_SUFFIXES)})::INT]
        ) {where}
        """


class UserAgentFaker(ArrayFaker):
    name = "Native user_agent"
    array = USER_AGENTS


class UrlFaker(NativeFaker):
    name = "Native url"

    def command(self, where):
        domains = _sql_array(["example.com", "test.org", "sample.net", "demo.io", "fake.co"])
        paths = _sql_array(["page", "article", "post", "item", "resource", "content", "data"])
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(
            'https://',
            ({domains})[1 + FLOOR(RANDOM() * 5)::INT], '/',
            ({paths})[1 + FLOOR(RANDOM() * 7)::INT], '/',
            FLOOR(RANDOM() * 10000)::INT
        ) {where}
        """


class UrlImageFaker(NativeFaker):
    name = "Native url_image"

    def command(self, where):
        domains = _sql_array(["example.com", "images.test.org", "cdn.sample.net"])
        exts = _sql_array(["jpg", "png", "gif", "webp"])
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(
            'https://',
            ({domains})[1 + FLOOR(RANDOM() * 3)::INT], '/img/',
            SUBSTRING(MD5(RANDOM()::TEXT), 1, 12), '.',
            ({exts})[1 + FLOOR(RANDOM() * 4)::INT]
        ) {where}
        """


class FilenameFaker(NativeFaker):
    name = "Native filename"

    def command(self, where):
        ext_arr = _sql_array(FILE_EXTENSIONS)
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=CONCAT(
            SUBSTRING(MD5(RANDOM()::TEXT), 1, 8), '.',
            ({ext_arr})[1 + FLOOR(RANDOM() * {len(FILE_EXTENSIONS)})::INT]
        ) {where}
        """


class TextShortFaker(NativeFaker):
    """Generate a short sentence (5-10 words) from lorem ipsum."""

    name = "Native text_short"

    def command(self, where):
        words_arr = _sql_array(LOREM_WORDS)
        n = len(LOREM_WORDS)
        # Generate 5-10 random words concatenated
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=INITCAP(CONCAT_WS(' ',
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT]
        )) || '.' {where}
        """


class TextFaker(NativeFaker):
    """Generate a paragraph (~20 words) from lorem ipsum."""

    name = "Native text"

    def command(self, where):
        words_arr = _sql_array(LOREM_WORDS)
        n = len(LOREM_WORDS)
        # Generate ~20 random words concatenated
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=INITCAP(CONCAT_WS(' ',
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT],
            ({words_arr})[1 + FLOOR(RANDOM() * {n})::INT]
        )) || '.' {where}
        """


class SlugFaker(NativeFaker):
    """Generate a slug from source column or random if no dependency."""

    name = "Native slug"
    depends = None

    def __init__(self, schema, table, column, args):
        super().__init__(schema, table, column, args)
        if args and args[0]:
            self.depends = args[0]

    def command(self, where):
        if self.depends:
            # Slugify the source column: lowercase, remove non-alphanumeric except spaces/hyphens,
            # replace spaces with hyphens, collapse multiple hyphens
            return f"""
            UPDATE {self.schema}.{self.table} SET {self.column}=LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE({self.depends}, '[^a-zA-Z0-9\\s-]', '', 'g'),
                        '\\s+', '-', 'g'
                    ),
                    '-+', '-', 'g'
                )
            ) {where}
            """
        else:
            # Random slug using MD5
            return f"""
            UPDATE {self.schema}.{self.table} SET {self.column}=LOWER(SUBSTRING(MD5(RANDOM()::TEXT), 1, 15)) {where}
            """


class RandomSlugFaker(NativeFaker):
    """Generate a random slug (for password, username without dependency)."""

    name = "Native random_slug"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=LOWER(SUBSTRING(MD5(RANDOM()::TEXT), 1, 15)) {where}
        """


class SerialFaker(NativeFaker):
    """Generate sequential integers using a temporary sequence."""

    name = "Native serial"

    def __init__(self, schema, table, column, args):
        super().__init__(schema, table, column, args)
        # Sanitize sequence name (replace dots with underscores)
        self.seq_name = f"seq_{schema}_{table}_{column}".replace(".", "_")
        self.start_value = 200000000

    def setup_command(self):
        """Return command to create the temp sequence."""
        return f"CREATE TEMP SEQUENCE IF NOT EXISTS {self.seq_name} START {self.start_value}"

    def command(self, where):
        return f"""
        UPDATE {self.schema}.{self.table} SET {self.column}=NEXTVAL('{self.seq_name}') {where}
        """


FAKERS = {
    "person_firstname": PersonFirstNameFaker,
    "person_familyname": PersonFamilyNameFaker,
    "person_name": PersonNameFaker,
    "tla": TlaFaker,
    "business_name": BusinessNameFaker,
    "slug": SlugFaker,
    "null": NullFaker,
    "text_short": TextShortFaker,
    "text": TextFaker,
    "email": EmailFaker,
    "user_agent": UserAgentFaker,
    "url": UrlFaker,
    "url_image": UrlImageFaker,
    "phonenumber": PhoneNumberFaker,
    "address": AddressFaker,
    "city": CityFaker,
    "zipcode": ZipcodeFaker,
    "filename": FilenameFaker,
    "inet_addr": InetAddrFaker,
    "username": SlugFaker,
    "int": IntFaker,
    "password": RandomSlugFaker,
    "serial": SerialFaker,
    "static_str": StaticStringFaker,
}


def get_mapper(
    table_schema, table_name, column_name, data_type, pii_type, args, depends, **_
):
    return FAKERS[pii_type](
        schema=table_schema,
        table=table_name,
        column=column_name,
        args=args.split(",") if args else [],
    )


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

    row_mapper = RowMapper(table, pii_spec)
    if not row_mapper.mappers and not row_mapper.natives:
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
    for native in row_mapper.natives:
        print(f"Executing {native}")
        # Run setup command if the native has one (e.g., create sequence)
        if hasattr(native, "setup_command"):
            write_cursor.execute(native.setup_command())
        if keepers:
            where = f"WHERE {pks[0]} NOT IN ({','.join([p]*len(keepers))})"
            write_cursor.execute(native.command(where), keepers)
        else:
            write_cursor.execute(native.command(""))

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
            LOG.exception(
                f"Table: {table}\n SQL: {sql} \n Values: {values}\n Spec: {pii_spec}"
            )
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
        with open(keep_filename) as f:
            keep = yaml.safe_load(f)
    if not fixed_filename:
        fixed = {}
    else:
        with open(fixed_filename) as f:
            fixed = yaml.safe_load(f)
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


def migrate_json_to_yaml(json_file):
    """Convert JSON config file to YAML format."""
    with open(json_file) as f:
        data = json.load(f)
    yaml.dump(data, sys.stdout, default_flow_style=False, allow_unicode=True)


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
        help='YAML file mapping rows to not mask {"schema.table": ["pk1", "pk2"]}',
        default=None,
    )
    parser.add_argument(
        "--fixed",
        type=str,
        help='YAML file mapping fixed masks: {"schema.table": {"pk1": {"col": "val"}]}',
        default=None,
    )
    parser.add_argument("-e", "--executors", type=int, default=2)
    parser.add_argument(
        "--migrate",
        type=str,
        metavar="JSON_FILE",
        help="Convert a JSON config file to YAML (prints to stdout)",
    )
    args = parser.parse_args()
    if args.list_fakers:
        print_fakers()
    elif args.migrate:
        migrate_json_to_yaml(args.migrate)
    elif not args.filename and not args.dburl:
        parser.print_help()
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
