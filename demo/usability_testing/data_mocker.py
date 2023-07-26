from multiprocessing import Process
from faker import Faker
import re
import os
import click
import csv
import json
from typing import Optional
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import fastparquet as fp


# to support save csv, and faster parquet, we don't use faker-cli directly
# but the design is similar, thanks for the author
fake = Faker()
def fake_write(writer, num_rows, col_types):
    for i in range(num_rows):
        # TODO: Handle args
        row = [ fake.format(ctype) for ctype in col_types ]
        writer.write(row)

class Writer:
    def __init__(self, output, headers, filename: Optional[str] = None):
        self.output = output
        self.headers = headers
        self.writer = None

    def write(self, row):
        pass

    def close(self):
        pass


class CSVWriter(Writer):
    def __init__(self, output, headers, filename):
        super().__init__(output, headers)
        self.writer = csv.writer(self.output)
        self.write(headers)

    def write(self, row):
        self.writer.writerow(row)

class ParquetWriter(Writer):
    def __init__(self, output, headers, filename, types):
        super().__init__(output, headers)
        self.filename = filename
        self.table: pa.Table = None
        self.append_dicts = {}
        # sql types to dtype
        DTYPES = {
            'smallint': "int16",
            "int": "int32",
            'string':'str',
            'bigint': "int64",
            "date": "datetime64[ns]",
            "timestamp": "datetime64[ns]",
            "float":"float32",
            'double':"float64",
        }
        self.append_rows = []
        self.types = types
        self.dtype = [(k, v) for k,v in zip(self.headers, [DTYPES[t] if t in DTYPES else t for t in types])]
        print(self.dtype)

    def write(self, row):
        """concat_tables is slow, so we store rows in cache"""
        self.append_rows.append(tuple(row))

    def close(self):
        print('write data to file')
        # print(self.append_rows)
        data = np.array(self.append_rows, dtype=self.dtype)
        df = pd.DataFrame(data)
        # speical for date type
        for c, t in zip(self.headers, self.types):
            if t == 'date':
                df[c] = df[c].dt.date
        print(df)
        # engine can't use fastparquet when use_deprecated_int96_timestamps
        # df.to_parquet(path, times="int96")
        # Which forwards the kwarg **{"times": "int96"} into fastparquet.writer.write(). 
        df.to_parquet(self.filename, use_deprecated_int96_timestamps=True)
        # print(df)
        # fp.write(self.filename, df, use_deprecated_int96_timestamps=True)

# TODO faster parquet faker? gen one column(x rows) in one time

KLAS_MAPPER = {
    "csv": CSVWriter,
    # "json": JSONWriter,
    "parquet": ParquetWriter,
    # "deltalake": DeltaLakeWriter
}

def fake_file(num_rows, fmt, output, columns, fake_types):
    print(f'generate {output}')
    # columns [[c,t], [c,t,...]]
    headers = [ c[0] for c in columns ]
    if fmt == 'csv':
        with open(output, mode='w', newline='') as file:
            writer = KLAS_MAPPER.get(fmt)(file, headers, output)
            fake_write(writer, num_rows, fake_types)
    elif fmt == 'parquet':
        import sys
        writer = ParquetWriter(sys.stdout, headers, output, [c[1] for c in columns])
        fake_write(writer, num_rows, fake_types)
    else:
        assert False, "fmt unsupported"
    writer.close()


@click.command()
@click.option("--num-files", "-nf", default=1, help="Number of files")
@click.option("--num-rows", "-n", default=1, help="Number of rows per file")
@click.option("--fmt", "-f", type=click.Choice(["csv", "json", "parquet"]), default="parquet", help="Format of the output")
@click.option("--output", "-o", type=click.Path(writable=True), help='output dir')
@click.option("--sql", "-s", help="create table sql/table schema part", default=None)
def main(num_files, num_rows, fmt, output, sql):
    if fmt in ['parquet'] and output is None:
        raise click.BadArgumentUsage("parquet formats requires --output/-o filename parameter.")

    # openmldb create table may has some problem, cut to simple style
    create_table_sql = sql # 'CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR, INDEX(foo=bar)) OPTIONS (type="kv")'
    regex = r'CREATE TABLE (\w+) \((.*?)\)' # no options
    match = re.search(regex, create_table_sql)
    if not match:
        cols = sql
    else:
        # columns, [index]
        table = match.group(1)
        cols_idx = match.group(2)
        cols = re.sub(r',[ *]INDEX\((.*)', '', cols_idx, flags= re.IGNORECASE)
    # parse schema from cols is enough, just use item[0]&[1] name&type
    cols = [ [c[0], c[1].lower()] for c in (c.strip().split(' ') for c in cols.split(',')) ]
    print(cols)
    # sql types to faker provider
    def type_converter(sql_type):
        # TODO(hw): max value
        # conv_map = {
        #     "smallint": 
        # }
        if sql_type.startswith('int') or sql_type in ['bigint', 'tinyint', 'smallint']:
            return 'pyint'
        if sql_type in ['varchar', 'string']:
            return 'pystr'
        if sql_type in ['date', 'timestamp']:
            return 'iso8601'
        if sql_type in ['float', 'double']:
            return 'pyfloat'
        return 'py' + sql_type

    types = [ type_converter(c[1]) for c in cols ]
    os.makedirs(output, exist_ok=True)
    # generate data in multi-processing
    import time
    start = time.time()
    for i in range(num_files):
        fake_file(num_rows, fmt, f'{output}/{int(start)}-{i}.{fmt}', cols, types)
    elap = time.time() - start
    print(f"elap {elap/60}min")

if __name__ == '__main__':
    main()
