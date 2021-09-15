import json
import sqlite3
import tempfile
from io import StringIO

import pytest
import slugify

from pgdatacleaner import cleaner

CSV = """table_schema;table_name;column_name;data_type;pii;pii_type;depends;args
main;t1;c1;serial;no;;;
main;t1;c2;name;yes;person_name;;
main;t1;c3;slug;yes;slug;c2;c2
"""


@pytest.fixture
def piis():
    return cleaner.get_piis(StringIO(CSV))


@pytest.fixture
def pii_dd_file():
    with tempfile.NamedTemporaryFile() as f:
        f.write(CSV.encode())
        f.flush()
        yield f.name


@pytest.fixture
def keeper_file():
    with tempfile.NamedTemporaryFile(mode="w+t") as f:
        json.dump({"main.t1": ["1"]}, f)
        f.flush()
        yield f.name


@pytest.fixture
def fixed_file():
    with tempfile.NamedTemporaryFile(mode="w+t") as f:
        json.dump({"main.t1": {1: {"c2": "fixed"}}}, f)
        f.flush()
        yield f.name


@pytest.fixture
def test_db():
    with tempfile.NamedTemporaryFile() as f:
        conn = sqlite3.connect(f.name)
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE main.t1 (c1 TEXT PRIMARY KEY, c2 TEXT, c3 TEXT)
        """
        )
        cur.execute(
            """
        INSERT INTO main.t1 VALUES (1, 'A name', 'a-name')
        """
        )
        cur.execute(
            """
        INSERT INTO main.t1 VALUES (2, 'A name 2', 'a-name')
        """
        )
        cur.execute(
            """
        INSERT INTO main.t1 VALUES (3, 'A name 3', 'a-name')
        """
        )
        conn.commit()
        conn.close()
        yield f


def test_get_piis(piis):
    assert piis["main.t1"]["c2"]
    assert "c1" not in piis["main.t1"]
    assert piis["main.t1"]["c3"][0]["args"] == "c2"
    assert piis["main.t1"]["c3"][0]["depends"] == "c2"


def test_clean_simple(piis):
    row_mapper = cleaner.RowMapper(piis["main.t1"])
    row = {"c1": "unmasked", "c2": "masked_name", "c3": "masked_with_arg"}
    masked = row_mapper.mask(row)
    assert masked["c3"] == slugify.slugify(masked["c2"])


def test_mask(piis, test_db):
    cleaner.mask_pii("main.t1", piis["main.t1"], test_db.name, None, None)
    conn = sqlite3.connect(test_db.name)
    conn.row_factory = sqlite3.Row
    data = conn.cursor().execute("SELECT * from t1 ORDER BY c1").fetchall()
    rows = [{k: r[k] for k in r.keys()} for r in data]
    print(rows)
    assert rows[0]["c2"] != "A name"
    assert rows[0]["c1"] == "1"


def test_mask_keep(piis, test_db):

    cleaner.mask_pii("main.t1", piis["main.t1"], test_db.name, keepers=[1], fixed=None)
    conn = sqlite3.connect(test_db.name)
    conn.row_factory = sqlite3.Row
    data = conn.cursor().execute("SELECT * from t1 ORDER BY c1").fetchall()
    rows = [{k: r[k] for k in r.keys()} for r in data]
    print(rows)
    assert rows[0]["c2"] == "A name"
    assert rows[0]["c1"] == "1"
    assert rows[1]["c2"] != "A name 2"
    assert rows[1]["c1"] == "2"


def test_fixed(piis, test_db):
    cleaner.mask_pii(
        "main.t1",
        piis["main.t1"],
        test_db.name,
        keepers=None,
        fixed={"1": {"c3": "fixed"}},
    )
    conn = sqlite3.connect(test_db.name)
    conn.row_factory = sqlite3.Row
    data = conn.cursor().execute("SELECT * from t1 ORDER BY c1").fetchall()
    rows = [{k: r[k] for k in r.keys()} for r in data]
    print(rows)
    assert rows[0]["c3"] == "fixed"
    assert rows[0]["c1"] == "1"


def test_keep_and_fixed(piis, test_db):
    cleaner.mask_pii(
        "main.t1",
        piis["main.t1"],
        test_db.name,
        keepers=["1"],
        fixed={"1": {"c3": "fixed"}},
    )
    conn = sqlite3.connect(test_db.name)
    conn.row_factory = sqlite3.Row
    data = conn.cursor().execute("SELECT * from t1 ORDER BY c1").fetchall()
    rows = [{k: r[k] for k in r.keys()} for r in data]
    print(rows)
    assert rows[0]["c3"] == "fixed"
    assert rows[0]["c2"] == "A name"
    assert rows[0]["c1"] == "1"


def test_command_simple(test_db, pii_dd_file):
    cleaner.clean(
        executors=2,
        filename=pii_dd_file,
        dburl=test_db.name,
        keep_filename=None,
        fixed_filename=None,
    )
    conn = sqlite3.connect(test_db.name)
    conn.row_factory = sqlite3.Row
    data = conn.cursor().execute("SELECT * from t1 ORDER BY c1").fetchall()
    rows = [{k: r[k] for k in r.keys()} for r in data]
    print(rows)
    assert rows[0]["c3"] == slugify.slugify(rows[0]["c2"])
    assert rows[0]["c2"] != "A name"
    assert rows[0]["c1"] == "1"


def test_command_fixed(test_db, pii_dd_file, fixed_file):
    cleaner.clean(
        executors=2,
        filename=pii_dd_file,
        dburl=test_db.name,
        keep_filename=None,
        fixed_filename=fixed_file,
    )
    conn = sqlite3.connect(test_db.name)
    conn.row_factory = sqlite3.Row
    data = conn.cursor().execute("SELECT * from t1 ORDER BY c1").fetchall()
    rows = [{k: r[k] for k in r.keys()} for r in data]
    print(rows)
    assert rows[0]["c2"] == "fixed"
    assert rows[0]["c1"] == "1"


def test_command_keepers(test_db, pii_dd_file, keeper_file):
    cleaner.clean(
        executors=2,
        filename=pii_dd_file,
        dburl=test_db.name,
        fixed_filename=None,
        keep_filename=keeper_file,
    )
    conn = sqlite3.connect(test_db.name)
    conn.row_factory = sqlite3.Row
    data = conn.cursor().execute("SELECT * from t1 ORDER BY c1").fetchall()
    rows = [{k: r[k] for k in r.keys()} for r in data]
    print(rows)
    assert rows[0]["c2"] == "A name"
    assert rows[0]["c1"] == "1"
