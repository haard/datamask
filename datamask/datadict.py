import argparse
import csv

import psycopg2
from psycopg2.extras import DictCursor


def get_schema(dburi: str, schema_name: str) -> list:
    with psycopg2.connect(dburi) as conn:
        cursor = conn.cursor(cursor_factory=DictCursor)
        cursor.execute(
            """
        select table_schema, table_name, column_name, data_type
        from information_schema.columns
        WHERE table_schema=%s AND is_updatable='YES'
        ORDER BY table_name, column_name
        """,
            (schema_name,),
        )
        return [dict(row.items()) for row in cursor]


def write_datadict(schema, destfile):

    writer = csv.DictWriter(
        destfile,
        "table_schema;table_name;column_name;data_type;pii;pii_type;args;depends".split(
            ";"
        ),
        delimiter=";",
    )
    writer.writeheader()
    for row in schema:
        writer.writerow(row)


def merge(original, new, default_pii=False):
    keyset = "table_schema", "table_name", "column_name"
    original_kv = {tuple(row[k] for k in keyset): row for row in original}
    new_kv = {tuple(row[k] for k in keyset): row for row in new}
    merged = dict(original_kv.items())
    for key in original_kv.keys():
        if key not in new_kv:
            print(f"Removing {'.'.join(key)}")
            del merged[key]
    for key in new_kv.keys():
        if key not in merged:
            print(f"Adding {'.'.join(key)}")
            merged[key] = new_kv[key]
            if default_pii:
                merged[key]["pii"] = "yes"

    return [merged[key] for key in sorted(merged.keys())]


def read_csv(srcfile):
    return [r for r in csv.DictReader(srcfile, delimiter=";")]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn", type=str)
    parser.add_argument("schema_name", type=str, default="public")
    parser.add_argument("outfile", type=argparse.FileType("wt"))
    parser.add_argument("-i", "--infile", type=argparse.FileType("rt"), nargs="?")

    # parser.add_argument('--existing', '-e', type=file, mode='rt',
    #                    default=None, help='Merge with existinc csv file')
    args = parser.parse_args()
    schema = get_schema(args.dsn, args.schema_name)
    if args.infile:
        merged = merge(read_csv(args.infile), schema)
        write_datadict(merged, args.outfile)
    else:
        write_datadict(schema, args.outfile)
