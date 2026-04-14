# datamask

Mask sensitive data in a PostgreSQL database (PII/PHI) for development/testing purposes.

Uses native PostgreSQL operations for masking - no data leaves the database.

## Installation

```bash
pip install datamask
```

## Usage

### 1. Create a data dictionary

Generate a CSV data dictionary from your database schema:

```bash
datadict 'postgresql://<user>:<password>@<host>/<database>' <schema> my_pii_dd.csv
```

Edit the CSV and set `pii` to `yes` for columns that need masking, and `pii_type` to one of the
available faker types. Run `datamask -l` to list all available fakers.

### 2. Mask the data

```bash
datamask -d 'postgresql://<user>:<password>@<host>/<database>' -f my_pii_dd.csv
```

### 3. Updating the data dictionary

When your schema changes, regenerate the data dictionary using your existing one as a seed:

```bash
datadict 'postgresql://<user>:<password>@<host>/<database>' <schema> -i my_existing_dd.csv my_new_pii_dd.csv
```

### Advanced options

Skip specific rows from masking using `--keep` with a YAML file:

```yaml
# keep.yaml
schema.table_name:
  - pk_value_1
  - pk_value_2
```

Set fixed values for specific rows using `--fixed` with a YAML file:

```yaml
# fixed.yaml
schema.table_name:
  pk_value:
    column_name: "fixed value"
```

## Available fakers

Run `datamask -l` to see all available faker types. Includes: `person_name`, `person_firstname`,
`person_familyname`, `email`, `address`, `city`, `zipcode`, `phonenumber`, `business_name`,
`username`, `password`, `url`, `url_image`, `inet_addr`, `text`, `text_short`, `filename`,
`slug`, `serial`, `int`, `tla`, `user_agent`, `static_str`, `null`.

## Caveats

Never run this against a production database. I'm not responsible for your data.

## License

MIT License - Copyright (c) 2021, Fredrik Håård
