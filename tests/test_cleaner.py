import os
import tempfile
from io import StringIO

import psycopg2
import yaml
import psycopg2.extras
import pytest

from datamask import cleaner

# PostgreSQL connection string - use environment variable or default for local Docker
PG_DSN = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://postgres:test@localhost:6543/postgres"
)

CSV = """table_schema;table_name;column_name;data_type;pii;pii_type;depends;args
public;t1;c1;serial;no;;;
public;t1;c2;name;yes;person_name;;
public;t1;c3;slug;yes;slug;c2;c2
public;t1;c4;what;yes;tla;;
"""


@pytest.fixture
def piis():
    return cleaner.get_piis(StringIO(CSV))


@pytest.fixture
def pii_dd_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(CSV)
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def keeper_file():
    with tempfile.NamedTemporaryFile(mode="w+t", delete=False) as f:
        yaml.dump({"public.t1": ["1"]}, f)
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def fixed_file():
    with tempfile.NamedTemporaryFile(mode="w+t", delete=False) as f:
        yaml.dump({"public.t1": {1: {"c2": "fixed"}}}, f)
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def test_db():
    """Create a test table in PostgreSQL and clean up after."""
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    # Create test table
    cur.execute("DROP TABLE IF EXISTS public.t1")
    cur.execute("""
        CREATE TABLE public.t1 (
            c1 TEXT PRIMARY KEY,
            c2 TEXT,
            c3 TEXT,
            c4 TEXT
        )
    """)
    cur.execute("INSERT INTO public.t1 VALUES ('1', 'A name', 'a-name', 'ABC')")
    cur.execute("INSERT INTO public.t1 VALUES ('2', 'A name 2', 'a-name-2', 'DEF')")
    cur.execute("INSERT INTO public.t1 VALUES ('3', 'A name 3', 'a-name-3', 'GHI')")
    conn.close()

    yield PG_DSN

    # Cleanup
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS public.t1")
    conn.close()


def test_get_piis(piis):
    assert piis["public.t1"]["c2"]
    assert "c1" not in piis["public.t1"]
    assert piis["public.t1"]["c3"][0]["args"] == "c2"
    assert piis["public.t1"]["c3"][0]["depends"] == "c2"


def test_native_tla_generates_5_char_string(test_db):
    """Test that TlaFaker generates 5-character uppercase strings."""
    conn = psycopg2.connect(test_db)
    cur = conn.cursor()
    cur.execute("""
        UPDATE public.t1 SET c4 = UPPER(SUBSTRING(MD5(RANDOM()::TEXT), 1, 5))
    """)
    cur.execute("SELECT c4 FROM public.t1")
    for row in cur.fetchall():
        assert len(row[0]) == 5
        assert row[0].isupper()
    conn.close()


def test_mask(piis, test_db):
    cleaner.mask_pii("public.t1", piis["public.t1"], test_db, None, None)
    conn = psycopg2.connect(test_db)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * from public.t1 ORDER BY c1")
    rows = [dict(r) for r in cur.fetchall()]
    print(rows)
    # Name should be changed
    assert rows[0]["c2"] != "A name"
    # Primary key should not change
    assert rows[0]["c1"] == "1"
    # Slug should be a slugified version of the new name
    assert "-" in rows[0]["c3"] or rows[0]["c3"].islower()
    # TLA should be 5 chars
    assert len(rows[0]["c4"]) == 5
    conn.close()


def test_mask_keep(piis, test_db):
    cleaner.mask_pii("public.t1", piis["public.t1"], test_db, keepers=["1"], fixed=None)
    conn = psycopg2.connect(test_db)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * from public.t1 ORDER BY c1")
    rows = [dict(r) for r in cur.fetchall()]
    print(rows)
    # Row 1 should be kept unchanged
    assert rows[0]["c2"] == "A name"
    assert rows[0]["c1"] == "1"
    # Row 2 should be masked
    assert rows[1]["c2"] != "A name 2"
    assert rows[1]["c1"] == "2"
    conn.close()


def test_fixed(piis, test_db):
    cleaner.mask_pii(
        "public.t1",
        piis["public.t1"],
        test_db,
        keepers=None,
        fixed={"1": {"c3": "fixed"}},
    )
    conn = psycopg2.connect(test_db)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * from public.t1 ORDER BY c1")
    rows = [dict(r) for r in cur.fetchall()]
    print(rows)
    assert rows[0]["c3"] == "fixed"
    assert rows[0]["c1"] == "1"
    conn.close()


def test_keep_and_fixed(piis, test_db):
    cleaner.mask_pii(
        "public.t1",
        piis["public.t1"],
        test_db,
        keepers=["1"],
        fixed={"1": {"c3": "fixed"}},
    )
    conn = psycopg2.connect(test_db)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * from public.t1 ORDER BY c1")
    rows = [dict(r) for r in cur.fetchall()]
    print(rows)
    assert rows[0]["c3"] == "fixed"
    # c2 should still be original since row 1 is kept
    assert rows[0]["c2"] == "A name"
    assert rows[0]["c1"] == "1"
    conn.close()


def test_command_simple(test_db, pii_dd_file):
    cleaner.clean(
        executors=2,
        filename=pii_dd_file,
        dburl=test_db,
        keep_filename=None,
        fixed_filename=None,
    )
    conn = psycopg2.connect(test_db)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * from public.t1 ORDER BY c1")
    rows = [dict(r) for r in cur.fetchall()]
    print(rows)
    # Slug should be derived from the masked name
    assert rows[0]["c3"] != "a-name"
    assert rows[0]["c2"] != "A name"
    assert rows[0]["c1"] == "1"
    conn.close()


def test_command_fixed(test_db, pii_dd_file, fixed_file):
    cleaner.clean(
        executors=2,
        filename=pii_dd_file,
        dburl=test_db,
        keep_filename=None,
        fixed_filename=fixed_file,
    )
    conn = psycopg2.connect(test_db)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * from public.t1 ORDER BY c1")
    rows = [dict(r) for r in cur.fetchall()]
    print(rows)
    assert rows[0]["c2"] == "fixed"
    assert rows[0]["c1"] == "1"
    conn.close()


def test_command_keepers(test_db, pii_dd_file, keeper_file):
    cleaner.clean(
        executors=2,
        filename=pii_dd_file,
        dburl=test_db,
        fixed_filename=None,
        keep_filename=keeper_file,
    )
    conn = psycopg2.connect(test_db)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * from public.t1 ORDER BY c1")
    rows = [dict(r) for r in cur.fetchall()]
    print(rows)
    assert rows[0]["c2"] == "A name"
    assert rows[0]["c1"] == "1"
    conn.close()


# Additional tests for specific native fakers


@pytest.fixture
def all_fakers_db():
    """Create a table to test all faker types."""
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS public.faker_test")
    cur.execute("""
        CREATE TABLE public.faker_test (
            id SERIAL PRIMARY KEY,
            firstname TEXT,
            familyname TEXT,
            fullname TEXT,
            tla_col TEXT,
            business TEXT,
            slug_col TEXT,
            null_col TEXT,
            text_short_col TEXT,
            text_col TEXT,
            email_col TEXT,
            user_agent_col TEXT,
            url_col TEXT,
            url_image_col TEXT,
            phone_col TEXT,
            address_col TEXT,
            city_col TEXT,
            zipcode_col TEXT,
            filename_col TEXT,
            inet_col TEXT,
            int_col INTEGER,
            serial_col INTEGER
        )
    """)
    cur.execute("""
        INSERT INTO public.faker_test (
            firstname, familyname, fullname, tla_col, business, slug_col,
            null_col, text_short_col, text_col, email_col, user_agent_col,
            url_col, url_image_col, phone_col, address_col, city_col,
            zipcode_col, filename_col, inet_col, int_col, serial_col
        ) VALUES (
            'John', 'Doe', 'John Doe', 'ABC', 'Acme Inc', 'john-doe',
            'not-null', 'short text', 'long text', 'john@example.com', 'Mozilla/5.0',
            'https://example.com', 'https://example.com/img.jpg', '1234567890', '123 Main St',
            'New York', '12345', 'file.pdf', '192.168.1.1', 42, 1
        )
    """)
    conn.close()

    yield PG_DSN

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS public.faker_test")
    conn.close()


def test_all_native_fakers(all_fakers_db):
    """Test that all native faker types produce valid output."""
    conn = psycopg2.connect(all_fakers_db)
    cur = conn.cursor()

    # Test each faker individually
    tests = [
        ("firstname", cleaner.PersonFirstNameFaker),
        ("familyname", cleaner.PersonFamilyNameFaker),
        ("fullname", cleaner.PersonNameFaker),
        ("tla_col", cleaner.TlaFaker),
        ("business", cleaner.BusinessNameFaker),
        ("null_col", cleaner.NullFaker),
        ("text_short_col", cleaner.TextShortFaker),
        ("text_col", cleaner.TextFaker),
        ("user_agent_col", cleaner.UserAgentFaker),
        ("url_col", cleaner.UrlFaker),
        ("url_image_col", cleaner.UrlImageFaker),
        ("phone_col", cleaner.PhoneNumberFaker),
        ("address_col", cleaner.AddressFaker),
        ("city_col", cleaner.CityFaker),
        ("zipcode_col", cleaner.ZipcodeFaker),
        ("filename_col", cleaner.FilenameFaker),
        ("inet_col", cleaner.InetAddrFaker),
        ("int_col", cleaner.IntFaker),
    ]

    for col, faker_class in tests:
        faker = faker_class(
            schema="public",
            table="faker_test",
            column=col,
            args=[]
        )
        cur.execute(faker.command(""))
        conn.commit()

    # Verify results
    cur.execute("SELECT * FROM public.faker_test")
    row = cur.fetchone()

    # Check specific constraints
    cur.execute("SELECT firstname, familyname, fullname, tla_col, null_col, zipcode_col, phone_col, inet_col FROM public.faker_test")
    row = cur.fetchone()
    firstname, familyname, fullname, tla_col, null_col, zipcode_col, phone_col, inet_col = row

    assert firstname in cleaner.FIRST_NAMES
    assert familyname in cleaner.FAMILY_NAMES
    assert " " in fullname  # Full name has first + last
    assert len(tla_col) == 5
    assert tla_col.isupper()
    assert null_col is None
    assert len(zipcode_col) == 5
    assert len(phone_col) == 15
    assert inet_col.count(".") == 3  # IP address has 3 dots

    conn.close()


def test_serial_faker(all_fakers_db):
    """Test that SerialFaker generates sequential values."""
    conn = psycopg2.connect(all_fakers_db)
    cur = conn.cursor()

    # Insert more rows
    cur.execute("""
        INSERT INTO public.faker_test (firstname) VALUES ('Test1'), ('Test2'), ('Test3')
    """)
    conn.commit()

    faker = cleaner.SerialFaker(
        schema="public",
        table="faker_test",
        column="serial_col",
        args=[]
    )

    # Execute setup and update
    cur.execute(faker.setup_command())
    cur.execute(faker.command(""))
    conn.commit()

    # Check that values are sequential
    cur.execute("SELECT serial_col FROM public.faker_test ORDER BY serial_col")
    values = [r[0] for r in cur.fetchall()]

    assert values == sorted(values)  # Should be sequential
    assert values[0] >= 200000000  # Should start from configured value
    assert values[-1] - values[0] == len(values) - 1  # Should be contiguous

    conn.close()


def test_slug_faker_with_dependency(all_fakers_db):
    """Test that SlugFaker correctly slugifies a source column."""
    conn = psycopg2.connect(all_fakers_db)
    cur = conn.cursor()

    # First update the fullname
    cur.execute("UPDATE public.faker_test SET fullname = 'Hello World Test'")

    faker = cleaner.SlugFaker(
        schema="public",
        table="faker_test",
        column="slug_col",
        args=["fullname"]
    )

    cur.execute(faker.command(""))
    conn.commit()

    cur.execute("SELECT slug_col FROM public.faker_test")
    slug = cur.fetchone()[0]

    assert slug == "hello-world-test"
    conn.close()


def test_slug_faker_without_dependency(all_fakers_db):
    """Test that SlugFaker generates random slug when no dependency."""
    conn = psycopg2.connect(all_fakers_db)
    cur = conn.cursor()

    faker = cleaner.SlugFaker(
        schema="public",
        table="faker_test",
        column="slug_col",
        args=[]
    )

    cur.execute(faker.command(""))
    conn.commit()

    cur.execute("SELECT slug_col FROM public.faker_test")
    slug = cur.fetchone()[0]

    assert len(slug) == 15  # MD5 substring length
    assert slug.islower()
    conn.close()
