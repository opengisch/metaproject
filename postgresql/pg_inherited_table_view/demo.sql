

CREATE TABLE vehicle (
	id serial PRIMARY KEY,
	year smallint,
	year_end smallint,
	model_name text);

CREATE TABLE motorbike_brand ( id integer PRIMARY KEY, name text );
CREATE TABLE car_brand ( id integer PRIMARY KEY, name text );


CREATE TABLE motorbike (
	id integer REFERENCES vehicle,
	fk_brand integer REFERENCES motorbike_brand,
	max_speed smallint); -- not in top class, as some type of vehicle might not have max_speed

CREATE TABLE car (
	id integer REFERENCES vehicle,
	fk_brand integer REFERENCES car_brand,
	max_speed smallint);


INSERT INTO motorbike_brand (id, name) VALUES (1, 'BMW');
INSERT INTO motorbike_brand (id, name) VALUES (2, 'Triumph');

INSERT INTO car_brand (id, name) VALUES (1, 'Aston Martin');
INSERT INTO car_brand (id, name) VALUES (2, 'Ferrari');


SELECT fn_inherited_table_view(
	'{
		"vehicle": {
			"table_name":"vehicle",
			"pkey": "id",
			"pkey_value":"nextval(''vehicle_id_seq'')",
			"inherited_by": {
				"car": {
					"table_name":"car",
					"pkey": "id",
					"remap": {
						"fk_brand": "fk_car_brand"
					}
				},
				"bike": {
					"table_name":"motorbike",
					"pkey": "id",
					"remap": {
						"fk_brand": "fk_bike_brand"
					}
				}
			},
			"merge_view": {
				"view_name":"vw_vehicle_all",
				"destination_schema": "public",
				"additional_columns": {
					"for_sale": "year_end IS NULL OR year_end >= extract(year from now())"
				},
				"allow_type_change": true,
				"merge_columns": {
					"top_speed": {
						"car": "max_speed",
						"bike": "max_speed"
					}
				}
			}
		}
	}'::json
);

/* insert through the view between parent table and child table */
INSERT INTO vw_vehicle_car ( model_name, fk_brand, year, year_end, max_speed ) VALUES ('DB5', 1, 1963, 1965, 230);
INSERT INTO vw_vehicle_bike ( model_name, fk_brand, year,  year_end, max_speed ) VALUES ('Bonneville', 2, 1963, 1975, 160);

/* insert through the merge view */
INSERT INTO vw_vehicle_all ( vehicle_type, model_name, fk_car_brand, year, year_end) VALUES ('car','308 GTS', 2, 1977, 1985 );
INSERT INTO vw_vehicle_all ( vehicle_type, model_name, fk_bike_brand, year, year_end, top_speed) VALUES ('bike','R12', 1, 1937, 1940, 110 );
INSERT INTO vw_vehicle_all ( vehicle_type, model_name, fk_bike_brand, year, top_speed) VALUES ('bike','R1200GS', 1, 2004, 208 );

/* update */
UPDATE vw_vehicle_all SET top_speed = 256 WHERE model_name = '308 GTS';

/* delete */
DELETE FROM vw_vehicle_all WHERE model_name = 'R12';

/* select */
-- SELECT * FROM vw_vehicle_all;

/* switching allow_type_change to false whould raise an error here */
UPDATE vw_vehicle_all SET vehicle_type = 'bike' WHERE model_name = 'DB5';
