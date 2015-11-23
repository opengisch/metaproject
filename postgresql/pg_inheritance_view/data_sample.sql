

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
