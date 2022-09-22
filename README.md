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
Stable, supports postgresql and sqlite3.
Consists of 45% todo's and hacks - still works.


## Release history

    * 1.1.4
      * Added static_str
      * Handle lists and dicsts as JSONB [] when writing back rows

## Caveats
I'm not responsible for your data. Never run this against a production database, unless you feel like testing your backup restore procedures.

## License

Copyright (c) 2021, Fredrik Håård

Do whatever you want, don't blame me. You may also use this software as licensed under the MIT or BSD licenses, or the more permissive license below:

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
