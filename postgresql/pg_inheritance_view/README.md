
## Use case

`PGInheritanceView` is a Python class which creates views and triggers
to view and edit data spread on several tables in a scenario of object-oriented data definition with inherited tables.

By inherited, we mean that there is a 1:1 relation between the parent and the child table using standard referencing mechanism. 
We are not dealing with PostgreSQL inheritance, which in itself is not capable of handling such scenario.

## Example

You have a `vehicle` table, with two inherited tables `car` and `motorbike`.
For cars and motorbikes, it will create a view for each that will allow editing the columns spread on the child and parent table (vehicle).
It will also create one large view which unites all children tables, that allow seeing and editing the whole available info for vehicles.


```
definition = """
alias: vehicle
table: vehicle
pkey: id
pkey_value: nextval('vehicle_id_seq')
schema: public
children:
  car:
	table: car
	pkey: id
	remap:
	  fk_brand: fk_car_brand
  bike:
	table: motorbike
	pkey: id
	remap:
	  fk_brand: fk_bike_brand
merge_view:
  name: vw_vehicle_all
  additional_columns:
	for_sale: year_end IS NULL OR year_end >= extract(year from now())
  allow_type_change: true
  merge_columns:
	top_speed:
	  car: max_speed
	  bike: max_speed
"""

print(PGInheritanceView(pg_service, definition).sql_all())
```


## Reference

* `alias` will be the alias of the parent table
* `table` is the schema-qualified name of the parent table
* `pkey` is the column of the primary key of the parent table
* `pkey_value` is the function returning the value to be used as a primary key
* `[pkey_value_create_entry]` if true, it means that calling the function `pkey_value`will actually itself create an entry in the parent table. This entry will hence be updated after the insertion.
* `[allow_parent_only]` If `False`, insertion to parent table only will be forbidden, a child entry will be required. `True` by default.
* `schema` is the schema
* `children` will contain all children tables
  * `table_alias` must be replaced by the alias for this child table
    * `table` is the schema-qualified name of the child table
    * `pkey` is the column of the primary key of the child table
    * `[remap]` will contain details about columns to be remaped (renamed)
      * use `original: renamed` for each
* `merge_view` will contain details about the big merged view (unites all tables)
  * `name` the name (not schema-qualified) of the view to be created
  * `[additional_columns]` contains possible additional columns to be added to view (read-only)
    * use `column_alias: column_definition`for each
  * `[allow_type_change]` if `True`, type change within children is allowed (a car can be updated to motorbike), `False` by default.
  * `[merge_columns]` lists columns to be merged
    * `alias` alias of the merged column
      * `table: column` replace table name and column name for each column to be merged



