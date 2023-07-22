from faker import Faker
fake = Faker()


# openmldb create table may has some problem, cut to simple style
create_table_sql = 'CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR, INDEX(foo=bar)) OPTIONS (type="kv")'
regex = r'CREATE TABLE (\w+) \((.*?)\)' # no options
match = re.search(regex, create_table_sql)
# columns, [index]
table = match.group(1)
cols_idx = match.group(2)
print(cols_idx)
cols = re.sub(r',[ *]INDEX\((.*)', '', cols_idx, flags= re.IGNORECASE)
print(cols)
# parse schema from cols is enough

# use sqlite engine, no need?
from sqlalchemy import create_engine, text
import re
engine = create_engine('sqlite:///:memory:', echo=True)
engine.execute(f"CREATE TABLE {table} ({cols})")
sql = text('SELECT * FROM users')

result = engine.execute(sql)

schema = result.cursor.description

print(schema)
