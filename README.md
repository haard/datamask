# pgdatacleaner
## Purpose
Mask sensetive data in a database (i.e. PII/PHI) so it can be used for development/testing purposes. 
It is meant to keep IDs, so databases that for some reason has PII data in PK/FKs won't work very well.
## Usage
First, create a data dictionary from an existing database:
```bash
datadict 'postgresql://<user>:<password>@<host>/<database>' <schema> my_pii_dd.csv 
```
You will need to edit this file and set `pii` to `yes` for any columns that need to be masked,
and the `pii_type` to one of the available types to generate fake data. `dataclean -h` will 
list all available fakers.

Then, run `dataclean` to modify a database masking data as specified in the CSV. This is not a very fast operation
on databases of any significant size.
```bash
dataclean -f 'postgresql://<user>:<password>@<host>/<database>' -f my_edited_pii_dd.csv 
```

If you change your schema, adding/modifying/deleting columns and tables, you can regenerate the data dictionary
using your last copy as a seed so you don't have to re-specify columns that have not changed:

```bash
datadict 'postgresql://<user>:<password>@<host>/<database>' <schema> -i my_existing_dd.csv my_new_pii_dd.csv 
```

## Status
Unstable, alpha, consisting of 50% todo's and hacks, but working. 

## Caveats
I'm not responsible for your data. Never run this against a production database, unless you feel like testing your backup restore procedures.
