#!/usr/bin/env python

#import imp
#import os
#import sys

import psycopg2

if __name__ == '__main__':
    if __package__ is None:
        import sys
        from os import path
        sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
        from pg_inheritance_view import PGInheritanceView
    else:
        from ..pg_inheritance_view import PGInheritanceView

#from ..pg_inheritance_view import PGInheritanceView
#pgiv = imp.load_source('', os.path.join(os.path.dirname(__file__), '../../metaproject/postgresql/pg_inheritance_view/pg_inheritance_view.py'))

pg_service = "pg_test"
test_name = "test_insert"


definition = """
vehicle:
table_name: vehicle
pkey: id
pkey_value: nextval('vehicle_id_seq')
schema: {0}
inherited_by:
  car:
    table_name: car
    pkey: id
    remap:
      fk_brand: fk_car_brand
  bike:
    table_name: motorbike
    pkey: id
    remap:
      fk_brand: fk_bike_brand
merge_view:
  view_name: vw_vehicle_all
  additional_columns:
    for_sale: year_end IS NULL OR year_end >= extract(year from now())
  allow_type_change: true
  merge_columns:
    top_speed:
      car: max_speed
      bike: max_speed
""".format(test_name)

conn = psycopg2.connect("service={0}".format(service))
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

cur.execute( "CREATE SCHEMA {0};".format(test_name) )
		
cur.execute( PGInheritanceView(pg_service, definition).sql_all() )

#  insert through the view between parent table and child table */
cur.execute( "INSERT INTO {0}.vw_vehicle_car ( model_name, fk_car_brand, year, year_end, max_speed ) VALUES ('DB5', 1, 1963, 1965, 230); ".format(test_name))
cur.execute( "INSERT INTO {0}.vw_vehicle_bike ( model_name, fk_bike_brand, year,  year_end, max_speed ) VALUES ('Bonneville', 2, 1963, 1975, 160);".format(test_name))

#  insert through the merge view */
cur.execute( "INSERT INTO {0}.vw_vehicle_all ( vehicle_type, model_name, fk_car_brand, year, year_end) VALUES ('car','308 GTS', 2, 1977, 1985 );".format(test_name))
cur.execute( "INSERT INTO {0}.vw_vehicle_all ( vehicle_type, model_name, fk_bike_brand, year, year_end, top_speed) VALUES ('bike','R12', 1, 1937, 1940, 110 );".format(test_name))
cur.execute( "INSERT INTO {0}.vw_vehicle_all ( vehicle_type, model_name, fk_bike_brand, year, top_speed) VALUES ('bike','R1200GS', 1, 2004, 208 );".format(test_name))

#  update */
cur.execute( "UPDATE vw_vehicle_all SET top_speed = 256 WHERE model_name = '308 GTS';".format(test_name))

#  delete */
cur.execute( "DELETE FROM vw_vehicle_all WHERE model_name = 'R12';".format(test_name))

#  select */
# SELECT * FROM vw_vehicle_all;

#  switching allow_type_change to false whould raise an error here */
cur.execute( "UPDATE vw_vehicle_all SET vehicle_type = 'bike' WHERE model_name = 'DB5';".format(test_name))
