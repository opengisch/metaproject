
## Use case

PG inherited table view is a PostgreSQL function to create views and triggers
to edit data spread on several tables in a scenario of object-oriented data definition with inherited tables.

## Example

You have a `vehicle` table, with two inherited tables `car` and `motorbike`.
For cars and motorbikes, it will create a view for each that will allow editing the columns spread on the child and parent table (vehicle).
It will also create one large view which unites all children tables, that allow seeing and editing the whole available info for vehicles.

Have a look at [demo.sql](https://github.com/opengisch/metaproject/blob/master/postgresql/pg_inherited_table_view/demo.sql)

## Reference

```
parent table alias				|*	"vehicle": {
								| 	
table name (schema specified)	|*		"table_name":"vehicle",
								| 	
primary key column				|*		"pkey": "id",
								| 	
function to get the PK value	|*		"pkey_value":"nextval(''vehicle_id_seq'')",
this can also be NEW.id if one	| 	
wants to use the column given  	| 	
								| 	
if true, the function takes		| 		"pkey_value_create_entry": true,
care of creating entry. Hence,	| 	
instead of inserting, updates.	| 	
								| 	
								|*		"inherited_by": {
								|*			"car": {
								|*				"table_name":"car",
								|*				"pkey": "id",
remap columns					| 				"remap": {
	original: renamed			| 					"fk_brand": "fk_car_brand"
								| 			}
								| 		},
								|*			"bike": {
								|*				"table_name":"motorbike",
								|*				"pkey": "id",
								| 			"remap": {
								| 				"fk_brand": "fk_bike_brand"
								| 			}
								| 		}
								| 	},
								| 		"merge_view": {
								|*			"view_name":"vw_vehicle_all",
								|*			"destination_schema": "public",
 definition of additional col.	| 		"additional_columns": {
	alias: defintion			| 			"for_sale": "year_end IS NULL OR year_end >= extract(year from now())"
								| 		},
								| 	
 if false, changing child type	| 		"allow_type_change": false,
 when updating is not allowed.	| 	
 default is false.				| 	
                                | 	
 allow inserting on parent		| 		"allow_parent_only": false,
 only. default is true.			| 	
								| 	
                                | 	
 merge columns in the view		| 		"merge_columns": {
	alias						| 			"top_speed": {
		table: column			| 				"car": "max_speed",
								| 				"bike": "max_speed"
								| 			}
								| 		}
								| 	}
*: mandatory elements
```

