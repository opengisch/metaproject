/*
	pg_inherited_table_view

	Denis Rouzaud
	18.09.2015
*/

CREATE OR REPLACE FUNCTION :SCHEMA.fn_inherited_table_view(_definition json) RETURNS void AS
$BODY$
	DECLARE
		_parent_table_alias text;
		_child_table_alias text;
		_parent_table json;
		_child_table json;
		_parent_field_array text[];
		_child_field_array text[];
		_child_field_remapped_array text[];
		_merged_field_list text;
		_merge_view json;
		_view_rootname text;
		_view_name text;
		_column text;
		_function_trigger text;
		_merge_view_name text;
		_merge_delete_cmd text;
		_merge_view_rootname text;
		_additional_column text;
		_sql_cmd text;
		_destination_schema text;
		_table_alias text;
		_column_alias text;
		_merged_column_aliases text;
		_merged_column_original text;
		_allow_parent_only boolean;
		_allow_type_change boolean;
		_i integer;
	BEGIN
		-- there must be only one parent table given (i.e. only 1 entry on the top level of _definition)
		_parent_table_alias := json_object_keys(_definition);
		RAISE NOTICE 'generates view for %' , _parent_table_alias;
		_parent_table := _definition->_parent_table_alias;

		_merge_view := _parent_table->'merge_view';
		_destination_schema := _parent_table->>'destination_schema';
		IF _destination_schema IS NULL THEN
			_destination_schema := 'public';
		END IF;

		_merge_view_rootname := _parent_table->'merge_view'->>'view_name';
		IF _merge_view_rootname IS NULL THEN
			_merge_view_rootname := format( 'vw_%I_merge', _parent_table_alias );
		END IF;
		_merge_view_name := _destination_schema||'.'||_merge_view_rootname;

		-- get array of fields for parent table
		EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _parent_table->>'table_name') INTO _parent_field_array;
		_parent_field_array := array_remove(_parent_field_array, (_parent_table->>'pkey')::text ); -- remove pkey from field list

  		-- CREATE VIEW AND TRIGGERS/RULES FOR 1:1 JOINED VIEW
  		FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_alias;
			RAISE NOTICE 'edit view for %', _child_table_alias;

			-- define view name
			_view_rootname := 'vw_'||_parent_table_alias||'_'||_child_table_alias;
			_view_name := _destination_schema||'.'||_view_rootname;
			_function_trigger := _destination_schema||'.ft_'||_parent_table_alias||'_'||_child_table_alias||'_insert';

			-- get array of fields for child table
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list


			-- CREATE VIEW
			RAISE NOTICE 'Create view: %', _view_name;
			_sql_cmd := format('
				CREATE OR REPLACE VIEW %1$s AS
					SELECT %2$I.%3$I'
				, _view_name --1
				, _parent_table_alias --2
				, _parent_table->>'pkey' --3
			);
			-- add columns
			FOREACH _column IN ARRAY _parent_field_array LOOP
				IF (_parent_table->'alter'->_column->>'read')::text IS NOT NULL THEN
					_sql_cmd := _sql_cmd || format(', %1$s(%2$I)'
						, _parent_table->'alter'->_column->>'read'
						, _column
					);
					IF (_parent_table->'remap'->>_column)::text IS NULL THEN
						_sql_cmd := _sql_cmd || format( ' AS %I', _column ); -- if no remap, reuse original column name
					END IF;
				ELSE
					_sql_cmd := _sql_cmd || format(', %1$I.%2$I', _parent_table_alias, _column);
				END IF;
				IF (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
					_sql_cmd := _sql_cmd || format( ' AS %I', _parent_table->'remap'->>_column );
				END IF;
			END LOOP;
			FOREACH _column IN ARRAY _child_field_array LOOP
				IF (_child_table->'alter'->_column->>'read')::text IS NOT NULL THEN
					_sql_cmd := _sql_cmd || format(', %1$s(%2$I)'
						, _child_table->'alter'->_column->>'read'
						, _column
					);
					IF (_child_table->'remap'->>_column)::text IS NULL THEN
						_sql_cmd := _sql_cmd || format( ' AS %I', _column ); -- if no remap, reuse original column name
					END IF;
				ELSE
					_sql_cmd := _sql_cmd || format(', %1$I.%2$I', _child_table_alias, _column);
				END IF;
				IF (_child_table->'remap'->>_column)::text IS NOT NULL THEN
					_sql_cmd := _sql_cmd || format( ' AS %I', _child_table->'remap'->>_column );
				END IF;
			END LOOP;
			_sql_cmd := _sql_cmd || format('
				FROM %2$s %4$I INNER JOIN %1$s %3$I ON %3$I.%5$I = %4$I.%6$I;'
				, (_parent_table->>'table_name')::regclass --1
				, (_child_table->>'table_name')::regclass --2
				, _parent_table_alias --3
				, _child_table_alias --4
				, _parent_table->>'pkey' --5
				, _child_table->>'pkey' --6
			);
			EXECUTE _sql_cmd;


			-- INSERT TRIGGER FUNCTION
			RAISE NOTICE '  trigger function';
			_sql_cmd := format('
				CREATE OR REPLACE FUNCTION %1$s()
					RETURNS trigger AS
					$$
					BEGIN'
				, _function_trigger --1
			);

			-- Allow to use function to get existing master entry
			IF (_parent_table->>'pkey_value_create_entry')::boolean IS TRUE THEN
				_sql_cmd := _sql_cmd || format('
							NEW.%3$I := %1$s;
							-- the function creates or gets a parent row. If it previously existed with another subtype, it should raise an exception
							IF (SELECT _oid IS NOT NULL FROM (%6$s) AS foo WHERE _oid = NEW.%3$I) THEN
								RAISE EXCEPTION ''Cannot insert %4$s as %5$s since it already has another subtype. ID: %%'', NEW.%3$I;
							END IF;
							UPDATE %2$s SET'
					, _parent_table->>'pkey_value' --1
					, (_parent_table->>'table_name')::regclass --2
					, (_parent_table->>'pkey')::text --3
					, _parent_table_alias --4
					, _child_table_alias --5
					, array_to_string( --6
						ARRAY(
							SELECT
								'SELECT '
								|| (_parent_table->'inherited_by'->foo.json_object_keys->>'pkey')
								|| ' AS _oid '
								|| ' FROM '
								|| (_parent_table->'inherited_by'->foo.json_object_keys->>'table_name' ) || '
								'
							FROM (
								SELECT json_object_keys(_parent_table->'inherited_by')
								)
							AS foo
						), ' UNION ') --6
					);
				FOREACH _column IN ARRAY _parent_field_array LOOP
					_sql_cmd = _sql_cmd || format('
								%1$I = %2$s NEW.%3$I %4$s, '
						, _column --1
						, ((_parent_table->'alter'->_column)::json->>'write')::text||'(' --2
						, CASE --3
							WHEN (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
								_parent_table->'remap'->>_column
							ELSE
								_column
							END --3
						, CASE WHEN ((_parent_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --4
					);
				END LOOP;
				_sql_cmd := left( _sql_cmd, -2 ); -- remove extra comma
				_sql_cmd := _sql_cmd || format('
							WHERE %1$I = NEW.%1$I;'
					, (_parent_table->>'pkey')::text --4
					);
			ELSE
				_sql_cmd := _sql_cmd || format('
							INSERT INTO %1$s ( %2$I %3$s ) VALUES ( %4$s'
					, (_parent_table->>'table_name')::regclass --1
					, (_parent_table->>'pkey')::text --2
					, CASE WHEN array_length(_parent_field_array, 1)>0 THEN ', '||array_to_string(_parent_field_array, ', ') ELSE '' END --3
					, _parent_table->>'pkey_value' --4
					);
				FOREACH _column IN ARRAY _parent_field_array LOOP
					_sql_cmd = _sql_cmd || format(', %1$s NEW.%2$I %3$s'
						, ((_parent_table->'alter'->_column)::json->>'write')::text||'(' --1
						, CASE --2
							WHEN (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
								_parent_table->'remap'->>_column
							ELSE
								_column
							END --2
						, CASE WHEN ((_parent_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --3
						);
				END LOOP;
				_sql_cmd := _sql_cmd || format('
							) RETURNING %1$I INTO NEW.%1$I;'
					, (_parent_table->>'pkey')::text --1
					);
			END IF;
			_sql_cmd := _sql_cmd || format('
						INSERT INTO %2$s ( %3$I %4$s ) VALUES ( NEW.%1$I'
				, (_parent_table->>'pkey')::text --1
				, (_child_table->>'table_name')::regclass --2
				, (_child_table->>'pkey')::text --3
				, CASE WHEN array_length(_child_field_array, 1)>0 THEN ', '||array_to_string(_child_field_array, ', ') ELSE '' END --4
			);
			FOREACH _column IN ARRAY _child_field_array LOOP
				_sql_cmd := _sql_cmd || format(', %1$s NEW.%2$I %3$s'
				, ((_child_table->'alter'->_column)::json->>'write')::text||'(' --1
				, CASE --2
					WHEN (_child_table->'remap'->>_column)::text IS NOT NULL THEN
						_child_table->'remap'->>_column
					ELSE
						_column
					END --2
				, CASE WHEN ((_child_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --3
			);
			END LOOP;
			_sql_cmd := _sql_cmd || ');
						RETURN NEW;
					END;
					$$
					LANGUAGE plpgsql;';

			EXECUTE _sql_cmd;

			-- insert trigger
			RAISE NOTICE '  trigger';
			EXECUTE format('
				DROP TRIGGER IF EXISTS %1$I ON %2$s;
				CREATE TRIGGER %1$I
					  INSTEAD OF INSERT
					  ON %2$s
					  FOR EACH ROW
					  EXECUTE PROCEDURE %3$s();',
				'tr_'||_view_rootname||'_insert', --1
				_view_name::regclass, --2
				_function_trigger::regproc --3
			);


			-- update rule
			RAISE NOTICE '  update rule';
			_sql_cmd := format('
				CREATE OR REPLACE RULE %1$I AS ON UPDATE TO %2$s DO INSTEAD
				('
				, 'rl_'||_view_rootname||'_update' --1
				, _view_name::regclass --2
			);
			IF array_length(_parent_field_array, 1)>0 THEN
				_sql_cmd := _sql_cmd || format('
						UPDATE %1$s SET '
				, (_parent_table->>'table_name')::regclass --1
				);
				FOREACH _column IN ARRAY _parent_field_array LOOP
					_sql_cmd = _sql_cmd || format('
								%1$I = %2$s NEW.%3$I %4$s, '
					, _column --1
					, ((_parent_table->'alter'->_column)::json->>'write')::text||'('
					, CASE --3
						WHEN (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
							_parent_table->'remap'->>_column
						ELSE
							_column
						END --3
					, CASE WHEN ((_parent_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --4
				);
				END LOOP;
				_sql_cmd := left( _sql_cmd, -2 ); -- remove extra comma
				_sql_cmd := _sql_cmd || format('
						WHERE %1$I = OLD.%1$I;'
					, (_parent_table->>'pkey')::text --1
				);
			END IF;
			IF array_length(_child_field_array, 1)>0 THEN
				_sql_cmd := _sql_cmd || format('
					UPDATE %1$s SET '
					, (_child_table->>'table_name')::regclass --1
				);
				FOREACH _column IN ARRAY _child_field_array LOOP
					_sql_cmd = _sql_cmd || format('
								%1$I = %2$s NEW.%3$I %4$s, '
					, _column --1
					, ((_child_table->'alter'->_column)::json->>'write')::text||'(' --2
					, CASE --3
						WHEN (_child_table->'remap'->>_column)::text IS NOT NULL THEN
							_child_table->'remap'->>_column
						ELSE
							_column
						END --3
					, CASE WHEN ((_child_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --4
				);
				END LOOP;
				_sql_cmd := left( _sql_cmd, -2 ); -- remove extra comma
				_sql_cmd := _sql_cmd || format('
					WHERE %1$I = OLD.%1$I;'
					, (_child_table->>'pkey')::text --1
				);
			END IF;
			_sql_cmd := _sql_cmd || ')';
			EXECUTE _sql_cmd;

			-- delete rule
			RAISE NOTICE '  delete rule';
			EXECUTE format('
				CREATE OR REPLACE RULE %1$I AS ON DELETE TO %2$s DO INSTEAD
				(
				DELETE FROM %3$s WHERE %4$I = OLD.%4$I;
				DELETE FROM %5$s WHERE %6$I = OLD.%6$I;
				)'
				, 'rl_'||_view_rootname||'_delete' --1
				, _view_name::regclass --2
				, (_child_table->>'table_name')::regclass --3
				, (_child_table->>'pkey')::text --4
				, (_parent_table->>'table_name')::regclass --5
				, (_parent_table->>'pkey')::text --6
			);
		END LOOP;




		-- ***************
		-- ***************
		-- ***************
		-- MERGCE VIEW
		-- do not create merge view if not defined
		IF (_parent_table->'merge_view') IS NULL THEN
			RETURN;
		END IF;

		_allow_parent_only := (_parent_table->'merge_view'->>'allow_parent_only')::boolean;
		_allow_type_change := (_parent_table->'merge_view'->>'allow_type_change')::boolean;
		IF _allow_parent_only IS NULL THEN
			_allow_parent_only := true;
		END IF;
		IF _allow_type_change IS NULL THEN
			_allow_type_change := false;
		END IF;
		-- CREATE ENUM


		EXECUTE format('CREATE TYPE %1$I.%2$s_type AS ENUM ( %3$s ''%4$s'');'
			, _destination_schema --1
			, _parent_table_alias --2
			, CASE WHEN _allow_parent_only IS TRUE THEN format('''%s'', ',_parent_table_alias) ELSE '' END--3
			, array_to_string(ARRAY( SELECT json_object_keys(_parent_table->'inherited_by')), ''', ''') --4
		);

		-- MERGE VIEW (ALL CHILDREN TABLES)
		-- use first column to define type of inherited table
		_sql_cmd := format('CREATE OR REPLACE VIEW %s AS SELECT
			CASE ', _merge_view_name); -- create field to determine inherited table

		FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_alias;
			_sql_cmd := _sql_cmd || format('
				WHEN %1$I.%2$I IS NOT NULL THEN %1$L::%3$I.%4$s_type '
				, _child_table_alias --1
				, (_child_table->>'pkey')::text --2
				, _destination_schema --3
				, _parent_table_alias --4
			);
		END LOOP;
		IF _allow_parent_only IS TRUE THEN
			_sql_cmd := _sql_cmd || format('
				ELSE ''%1$s''::%2$I.%1$s_type'
				, _parent_table_alias --1
				, _destination_schema --2
			);
		END IF;
		_sql_cmd := _sql_cmd || format('
			END AS %1$s_type,
			%1$I.%2$I'
			, _parent_table_alias --1
			, (_parent_table->>'pkey')::text --2
		);
		-- add parent table columns
		FOREACH _column IN ARRAY _parent_field_array LOOP
			IF (_parent_table->'alter'->_column->>'read')::text IS NOT NULL THEN
				_sql_cmd := _sql_cmd || format(', %1$s(%2$I)'
					, _parent_table->'alter'->_column->>'read'
					, _column
				);
				IF (_parent_table->'remap'->>_column)::text IS NULL THEN
					_sql_cmd := _sql_cmd || format( ' AS %I', _column ); -- if no remap, reuse original column name
				END IF;
			ELSE
				_sql_cmd := _sql_cmd || format(', %1$I.%2$I', _parent_table_alias, _column);
			END IF;
			IF (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
				_sql_cmd := _sql_cmd || format( ' AS %I', _parent_table->'remap'->>_column );
			END IF;
		END LOOP;
		-- additional columns if they exists
		FOR _additional_column IN SELECT json_object_keys(_parent_table->'merge_view'->'additional_columns') LOOP
			_sql_cmd := _sql_cmd || format(',
				%1$s AS %2$I'
				, _parent_table->'merge_view'->'additional_columns'->>_additional_column
				, _additional_column
			);
		END LOOP;
		-- merge columns if needed
		FOR _column_alias IN SELECT json_object_keys(_merge_view->'merge_columns') LOOP
			_sql_cmd := _sql_cmd || ',
				CASE';
			FOR _table_alias IN SELECT json_object_keys(_merge_view->'merge_columns'->_column_alias) LOOP
				_sql_cmd := _sql_cmd || format('
					WHEN %1$I.%2$I IS NOT NULL THEN %1$I.%3$I'
					, _table_alias --1
					, (_child_table->>'pkey')::text --2
					, (_merge_view->'merge_columns'->_column_alias->>_table_alias)::text --3
				);
			END LOOP;
			_sql_cmd := _sql_cmd || format('
				END AS %I'
				, _column_alias
			);
		END LOOP;
		-- add columns of children tables
		FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_alias;
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list
			-- remove columns which are merged
			FOR _column_alias IN SELECT json_object_keys(_merge_view->'merge_columns') LOOP
				FOR _table_alias IN SELECT json_object_keys(_merge_view->'merge_columns'->_column_alias) LOOP
					CONTINUE WHEN _table_alias <> _child_table_alias;
					_child_field_array := array_remove(_child_field_array, (_merge_view->'merge_columns'->_column_alias->>_table_alias)::text);
				END LOOP;
			END LOOP;
			-- alter/remap and add columns
			FOREACH _column IN ARRAY _child_field_array LOOP
				IF (_child_table->'alter'->_column->>'read')::text IS NOT NULL THEN
					_sql_cmd := _sql_cmd || format(', %1$s(%2$I)'
						, _child_table->'alter'->_column->>'read'
						, _column
					);
					IF (_child_table->'remap'->>_column)::text IS NULL THEN
						_sql_cmd := _sql_cmd || format( ' AS %I', _column ); -- if no remap, reuse original column name
					END IF;
				ELSE
					_sql_cmd := _sql_cmd || format(', %1$I.%2$I', _child_table_alias, _column);
				END IF;
				IF (_child_table->'remap'->>_column)::text IS NOT NULL THEN
					_sql_cmd := _sql_cmd || format( ' AS %I', _child_table->'remap'->>_column );
				END IF;
			END LOOP;
		END LOOP;
		-- from parent table
		_sql_cmd := _sql_cmd || format('
			FROM %1$s %2$I'
			, (_parent_table->>'table_name')::regclass
			, _parent_table_alias
		);
		-- from children tables (LEFT JOIN)
		FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_alias;
			_sql_cmd := _sql_cmd || format('
				LEFT JOIN %1$s %2$I ON %3$I.%4$I = %2$s.%5$I '
				, (_child_table->>'table_name')::regclass --1
				, _child_table_alias --2
				, _parent_table_alias --3
				, (_parent_table->>'pkey')::text --4
				, (_child_table->>'pkey')::text --5
			);
		END LOOP;
		-- additional join if they exist
		_sql_cmd := _sql_cmd || format(' %s', _parent_table->'merge_view'->>'additional_join' );
		-- run it
		EXECUTE( _sql_cmd );



		-- INSERT FUNCTION TRIGGER FOR MERGE VIEW
		_sql_cmd := format('
			CREATE OR REPLACE FUNCTION %1$s() RETURNS TRIGGER AS $$
			BEGIN'
			, _destination_schema||'.ft_'||_merge_view_rootname||'_insert' --1
			);
		-- Allow to use function to get existing master entry
		IF (_parent_table->>'pkey_value_create_entry')::boolean IS TRUE THEN
			_sql_cmd := _sql_cmd || format('
						NEW.%4$I := %1$s;
							-- the function creates or gets a parent row. If it previously existed with another subtype, it should raise an exception
							IF (SELECT %5$s_type NOT IN (NULL::%6$I.%5$s_type, ''%5$s''::%6$I.%5$s_type) FROM %3$s WHERE %3$s.%4$I = NEW.%4$I) THEN
							RAISE EXCEPTION ''Cannot insert %5$s as %% since it already has another subtype. ID: %%'', NEW.%5$s_type, NEW.%4$I;
						END IF;
						UPDATE %2$s SET '
				, _parent_table->>'pkey_value' --1
				, (_parent_table->>'table_name')::regclass --2
				, _merge_view_name --3
				, (_parent_table->>'pkey')::text --4
				, _parent_table_alias --5
				, _destination_schema --6
				);
			FOREACH _column IN ARRAY _parent_field_array LOOP
				_sql_cmd = _sql_cmd || format('%1$I = %2$s NEW.%3$I %4$s, '
					, _column --1
					, ((_parent_table->'alter'->_column)::json->>'write')::text||'(' --2
					, CASE --3
						WHEN (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
							_parent_table->'remap'->>_column
						ELSE
							_column
						END --3
					, CASE WHEN ((_parent_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --4
					);
			END LOOP;
			_sql_cmd := left(_sql_cmd,-2); --remove extra comma
			_sql_cmd := _sql_cmd || format('
						WHERE %1$I = NEW.%1$I;'
				, (_parent_table->>'pkey')::text --1
				);
		-- standard insert
		ELSE
			_sql_cmd := _sql_cmd || format('
				INSERT INTO %1$s ( %2$I %3$s ) VALUES ( %4$s'
				, (_parent_table->>'table_name')::regclass --1
				, (_parent_table->>'pkey')::text --2
				, CASE WHEN array_length(_parent_field_array, 1)>0 THEN  ', '||array_to_string(_parent_field_array, ', ') ELSE '' END --3
				, _parent_table->>'pkey_value' --4
				, CASE WHEN array_length(_parent_field_array, 1)>0 THEN  ', NEW.'||array_to_string(_parent_field_array, ', NEW.') ELSE '' END --5
				);
			FOREACH _column IN ARRAY _parent_field_array LOOP
				_sql_cmd = _sql_cmd || format(', %1$s NEW.%2$I %3$s '
					, ((_parent_table->'alter'->_column)::json->>'write')::text||'(' --1
					, CASE --2
						WHEN (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
							_parent_table->'remap'->>_column
						ELSE
							_column
						END --2
					, CASE WHEN ((_parent_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --3
					);
			END LOOP;
			_sql_cmd := _sql_cmd || format(' ) RETURNING %1$I INTO NEW.%1$I;'
				, (_parent_table->>'pkey')::text --1
				);
		END IF;
		_sql_cmd := _sql_cmd || '
			CASE';
		FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			-- create an array with fields of the child table
			_child_table := _parent_table->'inherited_by'->_child_table_alias;
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list
			-- remove columns which are merged
			FOR _column_alias IN SELECT json_object_keys(_merge_view->'merge_columns') LOOP
				FOR _table_alias IN SELECT json_object_keys(_merge_view->'merge_columns'->_column_alias) LOOP
					CONTINUE WHEN _table_alias <> _child_table_alias;
					_child_field_array := array_remove(_child_field_array, (_merge_view->'merge_columns'->_column_alias->>_table_alias)::text);
				END LOOP;
			END LOOP;
			-- insert from merge columns if they exist
			_merged_column_aliases := '';
			_merged_column_original := '';
			FOR _column_alias IN SELECT json_object_keys(_merge_view->'merge_columns') LOOP
				FOR _table_alias IN SELECT json_object_keys(_merge_view->'merge_columns'->_column_alias) LOOP
					CONTINUE WHEN _table_alias <> _child_table_alias;
					_merged_column_aliases := _merged_column_aliases || ', NEW.' || _column_alias;
					_merged_column_original := _merged_column_original || ', ' || (_merge_view->'merge_columns'->_column_alias->>_table_alias)::text;
				END LOOP;
			END LOOP;
			-- insert command
			_sql_cmd := _sql_cmd || format('
					WHEN NEW.%1$I::%7$I.%1$I = %2$L THEN INSERT INTO %3$s ( %4$I %8$s %5$s) VALUES (NEW.%6$I %9$s'
				, _parent_table_alias || '_type' --1
				, _child_table_alias::text --2
				, (_child_table->>'table_name')::regclass --3
				, (_child_table->>'pkey')::text --4
				, CASE WHEN array_length(_child_field_array, 1)>0 THEN  ', '||array_to_string(_child_field_array, ', ') ELSE '' END --5
				, (_parent_table->>'pkey')::text --6
				, _destination_schema --7
				, _merged_column_original --8
				, _merged_column_aliases -- 9
				);
			FOREACH _column IN ARRAY _child_field_array LOOP
				_sql_cmd = _sql_cmd || format(', %1$s NEW.%2$I %3$s '
					, ((_child_table->'alter'->_column)::json->>'write')::text||'(' --1
					, CASE --2
						WHEN (_child_table->'remap'->>_column)::text IS NOT NULL THEN
							_child_table->'remap'->>_column
						ELSE
							_column
						END --2
					, CASE WHEN ((_child_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --3
					);
			END LOOP;
			_sql_cmd := _sql_cmd || ');';
		END LOOP;
		IF _allow_parent_only IS FALSE THEN
			_sql_cmd := _sql_cmd || format('
					ELSE
						RAISE EXCEPTION ''Insert on %1$s only is not allowed.''
					USING HINT = ''It must have a sub-type'';'
				, _parent_table_alias
				);
		END IF;
		_sql_cmd := _sql_cmd || '
			END CASE;
			RETURN NEW;
			END;
			$$
			LANGUAGE plpgsql;';
		EXECUTE( _sql_cmd );

		EXECUTE format('
			DROP TRIGGER IF EXISTS %1$I ON %2$s;
			CREATE TRIGGER %1$I
				INSTEAD OF INSERT
				ON %2$s
				FOR EACH ROW
				EXECUTE PROCEDURE %3$s();',
			'tr_'||_merge_view_rootname||'_insert', --1
			_merge_view_name::regclass, --2
			(_destination_schema||'.ft_'||_merge_view_rootname||'_insert')::regproc --3
		);



		-- UPDATE FUNCTION TRIGGER FOR MERGE VIEW
		_sql_cmd := format('
			CREATE OR REPLACE FUNCTION %1$s() RETURNS TRIGGER AS $$
			BEGIN'
			, _destination_schema||'.ft_'||_merge_view_rootname||'_update' --1
		);
		IF array_length(_parent_field_array, 1)>0 THEN
			_sql_cmd := _sql_cmd || format('
				UPDATE %1$s SET '
				, (_parent_table->>'table_name')::regclass --1
			);
			FOREACH _column IN ARRAY _parent_field_array LOOP
				_sql_cmd = _sql_cmd || format('%1$I = %2$s NEW.%3$I %4$s, '
					, _column --1
					, ((_parent_table->'alter'->_column)::json->>'write')::text||'(' --2
					, CASE --3
						WHEN (_parent_table->'remap'->>_column)::text IS NOT NULL THEN
							_parent_table->'remap'->>_column
						ELSE
							_column
						END --3
					, CASE WHEN ((_parent_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --4
				);
			END LOOP;
			_sql_cmd := left( _sql_cmd, -2 ); --remove extra comma
			_sql_cmd := _sql_cmd || format('
					WHERE %1$I = OLD.%1$I;'
				, (_parent_table->>'pkey')::text --1
			);
		END IF;
		_sql_cmd := _sql_cmd || format('
			/* Allow change type */
			IF OLD.%1$I <> NEW.%1$I::%2$I.%1$I THEN '
			, _parent_table_alias || '_type' --1
			, _destination_schema --2
		);
		-- do not allow insert on parent only
		IF _allow_parent_only IS FALSE THEN
			_sql_cmd := _sql_cmd || format('
				IF NEW.%1$I IS NULL THEN
					RAISE EXCEPTION ''Insert on %2$s only is not allowed.''
						USING HINT = ''It must have a sub-type'';
				END IF;'
				, _parent_table_alias || '_type' --1
				, _parent_table_alias --2
			);
		END IF;
		-- allow type change
		IF _allow_type_change IS TRUE THEN
			_sql_cmd := _sql_cmd || ' CASE';
			FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
				_child_table := _parent_table->'inherited_by'->_child_table_alias;
				_sql_cmd := _sql_cmd || format('
					WHEN OLD.%1$I::%5$I.%1$I = %2$L THEN DELETE FROM %3$s WHERE %4$I = OLD.%4$I;'
					, _parent_table_alias || '_type' --1
					, _child_table_alias::text --2
					, (_child_table->>'table_name')::regclass --3
					, (_child_table->>'pkey')::text --4
					, _destination_schema --5
				);
			END LOOP;
			_sql_cmd := _sql_cmd || '
				END CASE;
				CASE';
			FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
				_child_table := _parent_table->'inherited_by'->_child_table_alias;
				_sql_cmd := _sql_cmd || format('
					WHEN NEW.%1$I::%6$I.%1$I = %2$L THEN INSERT INTO %3$s (%4$I) VALUES (OLD.%5$I);'
					, _parent_table_alias || '_type' --1
					, _child_table_alias::text --2
					, (_child_table->>'table_name')::regclass --3
					, (_child_table->>'pkey')::text --4
					, (_parent_table->>'pkey')::text --5
					, _destination_schema --6
				);
			END LOOP;
			_sql_cmd := _sql_cmd || '
				END CASE;';
		-- do not allow type change
		ELSE
			_sql_cmd := _sql_cmd || format('
				RAISE EXCEPTION ''Type change not allowed for %1$s''
					USING HINT = ''You cannot switch from '' || OLD.%1$s_type ||'' to ''||NEW.%1$s_type;'
				, _parent_table_alias
			);
		END IF;
		_sql_cmd := _sql_cmd || '
			END IF;
			CASE ';
		FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_alias;
			-- write list of fields for update command
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list
			-- remove columns which are merged
			FOR _column_alias IN SELECT json_object_keys(_merge_view->'merge_columns') LOOP
				FOR _table_alias IN SELECT json_object_keys(_merge_view->'merge_columns'->_column_alias) LOOP
					CONTINUE WHEN _table_alias <> _child_table_alias;
					_child_field_array := array_remove(_child_field_array, (_merge_view->'merge_columns'->_column_alias->>_table_alias)::text);
				END LOOP;
			END LOOP;
			-- update from merge columns if they exist
			_merged_field_list := NULL::text;
			FOR _column_alias IN SELECT json_object_keys(_merge_view->'merge_columns') LOOP
				FOR _table_alias IN SELECT json_object_keys(_merge_view->'merge_columns'->_column_alias) LOOP
					CONTINUE WHEN _table_alias <> _child_table_alias;
					_merged_field_list := format('%1$s
						%2$I = NEW.%3$I, '
						, _merged_field_list --1
						, (_merge_view->'merge_columns'->_column_alias->>_table_alias)::text --2
						, _column_alias --3
						);
				END LOOP;
			END LOOP;
			_sql_cmd := _sql_cmd || format('
				WHEN NEW.%1$I::%3$I.%1$I = %2$L THEN '
				, _parent_table_alias || '_type' --1
				, _child_table_alias::text --2
				, _destination_schema --3
				);
			IF array_length(_child_field_array,1)>0 OR _merged_field_list IS NOT NULL THEN
				IF array_length(_child_field_array,1)=0 THEN
					_merged_field_list := left( _merged_field_list, -2); --remove extra comma
				END IF;
				_sql_cmd := _sql_cmd || format('
					UPDATE %1$s SET %2$s'
					, (_child_table->>'table_name')::regclass --1
					, _merged_field_list
					);
				FOREACH _column IN ARRAY _child_field_array LOOP
					_sql_cmd = _sql_cmd || format('
								%1$I = %2$s NEW.%3$I %4$s, '
						, _column --1
						, ((_child_table->'alter'->_column)::json->>'write')::text||'('
						, CASE --3
							WHEN (_child_table->'remap'->>_column)::text IS NOT NULL THEN
								_child_table->'remap'->>_column
							ELSE
								_column
							END --3
						, CASE WHEN ((_child_table->'alter'->_column)::json->>'write')::text IS NOT NULL THEN ')' ELSE '' END --4
						);
				END LOOP;
				_sql_cmd := left( _sql_cmd, -2); -- remove extra comma
				_sql_cmd := _sql_cmd || format('
					WHERE %1$I = OLD.%1$I;'
					, (_child_table->>'pkey')::text --1
					);
			ELSE
				_sql_cmd := _sql_cmd || 'NULL;';
			END IF;
		END LOOP;
		_sql_cmd := _sql_cmd || '
			END CASE;
			RETURN NEW;
			END;
			$$
			LANGUAGE plpgsql;';
		EXECUTE( _sql_cmd );
		EXECUTE format('
			DROP TRIGGER IF EXISTS %1$I ON %2$s;
			CREATE TRIGGER %1$I
				  INSTEAD OF UPDATE
				  ON %2$s
				  FOR EACH ROW
				  EXECUTE PROCEDURE %3$s();',
			'tr_'||_merge_view_rootname||'_update', --1
			_merge_view_name::regclass, --2
			(_destination_schema||'.ft_'||_merge_view_rootname||'_update')::regproc --3
		);



		-- delete function trigger for merge view
		_sql_cmd := format('
			CREATE OR REPLACE RULE %1$I AS ON DELETE TO %2$s DO INSTEAD	(',
			'rl_'||_merge_view_rootname||'_delete', --1
			_merge_view_name::regclass --2
		);
		FOR _child_table_alias IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_alias;
			_sql_cmd := _sql_cmd || format('
				DELETE FROM %1$s WHERE %2$I = OLD.%2$I;
				'
				, (_child_table->>'table_name')::regclass --1
				, (_child_table->>'pkey')::text --2
			);
		END LOOP;
		_sql_cmd := _sql_cmd || format('
			DELETE FROM %1$s WHERE %2$I = OLD.%2$I;)'
			, (_parent_table->>'table_name')::regclass --1
			, (_parent_table->>'pkey')::text --2
		);
		EXECUTE _sql_cmd;

	END;
$BODY$
LANGUAGE plpgsql;
