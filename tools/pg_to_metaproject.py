import psycopg2
import json
from collections import OrderedDict

def add_field(fields_dict, fieldname, nullable, data_type):
    if fieldname not in fields_dict:
        fields_dict[fieldname]=dict()
    fields_dict[fieldname]['nullable']=True if nullable=='YES' else False
    fields_dict[fieldname]['data_type']=data_type

# Connect to the database
conn = psycopg2.connect("service=pg_qgep")

cur=conn.cursor()


# Select all fields to initialize the table metainformation
cur.execute("SELECT * FROM information_schema.columns WHERE table_schema='qgep'")
colnames = [desc[0] for desc in cur.description]
pg_fields = cur.fetchall()

tables=dict()

for f in pg_fields:
    tablename=f[colnames.index('table_name')]
    fieldname=f[colnames.index('column_name')]
    nullable=f[colnames.index('is_nullable')]
    data_type=f[colnames.index('udt_name')]

    if tablename not in tables:
        tables[tablename]=dict()
        tables[tablename]['fields'] = OrderedDict()

    add_field(tables[tablename]['fields'], fieldname, nullable, data_type)

# Find primary keys
cur.execute("""
SELECT tc.table_schema, tc.table_name, kc.column_name
FROM
    information_schema.table_constraints tc,
    information_schema.key_column_usage kc
WHERE
    tc.constraint_type = 'PRIMARY KEY'
    AND kc.table_name = tc.table_name
    AND kc.table_schema = tc.table_schema
    AND kc.constraint_name = tc.constraint_name
ORDER BY 1, 2;
""")
colnames = [desc[0] for desc in cur.description]
pg_pks = cur.fetchall()

for pk in pg_pks:
    table_name=pk[colnames.index('table_name')]
    column_name=pk[colnames.index('column_name')]

    try:
        tables[table_name]['fields'][column_name]['primary_key']=True
    except KeyError:
        pass

# Find foreign keys
cur.execute("""
SELECT
    tc.constraint_name, tc.table_name, kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY'
""")
colnames = [desc[0] for desc in cur.description]

pg_constraints = cur.fetchall()

for c in pg_constraints:
    table_name=c[colnames.index('table_name')]
    column_name=c[colnames.index('column_name')]
    foreign_table_name=c[colnames.index('foreign_table_name')]
    foreign_column_name=c[colnames.index('foreign_column_name')]

    fk=dict()
    fk['table']=foreign_table_name
    fk['column']=foreign_column_name

    if 'primary_key' in tables[table_name]['fields'][column_name]:
        tables[table_name]['inherits'] = foreign_table_name

    tables[table_name]['fields'][column_name]['references']=fk

# Assemble the metaproject information
mp=dict()
mp['tables'] = tables

with open('qgep_base.mp', 'w') as f:
    f.write(json.dumps(mp, indent=2))
