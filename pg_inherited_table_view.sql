/*
	pg_inherited_table_view

	Denis Rouzaud
	18.09.2015
*/

CREATE OR REPLACE FUNCTION :SCHEMA.fn_inherited_table_view(_definition json) RETURNS void AS
$BODY$
	DECLARE
		_parent_table_name text;
		_child_table_name text;
		_parent_table json;
		_child_table json;
		_parent_field_array text[];
		_child_field_array text[];
		_parent_field_list text;
		_child_field_list text;

		_view_rootname text;
		_view_name text;
		_function_trigger text;
		_merge_view_name text;
		_merge_delete_cmd text;
		_merge_view_rootname text;
		_additional_column text;
		_sql_cmd text;
		_destination_schema text;
	BEGIN
		-- there must be only one parent table given (i.e. only 1 entry on the top level of _definition)
		_parent_table_name := json_object_keys(_definition);
		RAISE NOTICE 'generates view for %' , _parent_table_name;
		_parent_table := _definition->_parent_table_name;

		_destination_schema := _parent_table->'merge_view'->>'destination_schema';

		-- get array of fields for parent table
		EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _parent_table->>'table_name') INTO _parent_field_array;
		_parent_field_array := array_remove(_parent_field_array, (_parent_table->>'pkey')::text ); -- remove pkey from field list

		-- create command of update rule for parent fiels
		SELECT array_to_string(f, ', ')
			FROM ( SELECT array_agg(f||' = NEW.'||f) AS f
			FROM unnest(_parent_field_array) AS f ) foo
			INTO _parent_field_list;

  		-- create view and triggers/rules for 1:1 joined view
  		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			RAISE NOTICE 'edit view for %', _child_table_name;

			-- define view name
			_view_rootname := 'vw_'||_parent_table_name||'_'||_child_table_name;
			_view_name := _destination_schema||'.'||_view_rootname;
			_function_trigger := _destination_schema||'.ft_'||_parent_table_name||'_'||_child_table_name||'_insert';

			-- get array of fields for child table
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list

			-- view
			EXECUTE format('
				CREATE OR REPLACE VIEW %1$s AS
					SELECT %6$I.%8$I, %2$s, %3$s
				FROM %5$s %7$I INNER JOIN %4$s %6$I ON %6$I.%8$I = %7$I.%9$I;'
				, _view_name --1
				, _parent_table_name ||'.' || array_to_string(_parent_field_array, ', '||_parent_table_name||'.') --2
				, _child_table_name  ||'.' || array_to_string(_child_field_array,  ', '||_child_table_name ||'.') --3
				, (_parent_table->>'table_name')::regclass --4
				, (_child_table->>'table_name')::regclass --5
				, _parent_table_name --6
				, _child_table_name --7
				, _parent_table->>'pkey' --8
				, _child_table->>'pkey' --9
			);

			-- insert trigger function
			RAISE NOTICE '  trigger function';
			EXECUTE format('
				CREATE OR REPLACE FUNCTION %1$s()
					RETURNS trigger AS
					$$
					BEGIN
						INSERT INTO %2$s ( %3$I, %4$s ) VALUES ( %5$s, %6$s ) RETURNING %3$I INTO NEW.%3$I;
						INSERT INTO %7$s ( %8$I, %9$s ) VALUES ( NEW.%3$I, %10$s );
						RETURN NEW;
					END;
					$$
					LANGUAGE plpgsql;'
				, _function_trigger --1
				, (_parent_table->>'table_name')::regclass --2
				, (_parent_table->>'pkey')::text --3
				, array_to_string(_parent_field_array, ', ') --4
				, _parent_table->>'pkey_nextval' --5
				, 'NEW.'||array_to_string(_parent_field_array, ', NEW.') --6
				, (_child_table->>'table_name')::regclass --7
				, (_child_table->>'pkey')::text --8
				, array_to_string(_child_field_array, ', ') --9
				, 'NEW.'||array_to_string(_child_field_array, ', NEW.') --10
			);

			-- insert trigger
			RAISE NOTICE '  trigger';
			EXECUTE format('
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
			SELECT array_to_string(f, ', ') -- create command of update rule for parent fiels
				FROM ( SELECT array_agg(f||' = NEW.'||f) AS f
				FROM unnest(_child_field_array)     AS f ) foo
				INTO _child_field_list;
			EXECUTE format('
				CREATE OR REPLACE RULE %1$I AS ON UPDATE TO %2$s DO INSTEAD
				(
				UPDATE %3$s SET %4$s WHERE %5$I = OLD.%5$I;
				UPDATE %6$s SET %7$s WHERE %8$I = OLD.%8$I;
				)'
				, 'rl_'||_view_rootname||'_update' --1
				, _view_name::regclass --2
				, (_parent_table->>'table_name')::regclass --3
				, _parent_field_list --4
				, (_parent_table->>'pkey')::text --5
				, (_child_table->>'table_name')::regclass --6
				, _child_field_list --7
				, (_child_table->>'pkey')::text --8
			);

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


		-- create enum
		EXECUTE format('CREATE TYPE %1$I.%2$s_type AS ENUM (''%3$s'');'
			, _destination_schema
			, _parent_table_name
			, array_to_string(ARRAY( SELECT json_object_keys(_parent_table->'inherited_by')), ''', ''')
		);

		-- merge view (all children tables)
		_merge_view_rootname := _parent_table->'merge_view'->>'view_name';
		IF _merge_view_rootname IS NULL THEN
			_merge_view_rootname := format( 'vw_%I_merge', _parent_table_name );
		END IF;
		_merge_view_name := _destination_schema||'.'||_merge_view_rootname;
		-- create view and use first column to define type of inherited table
		_sql_cmd := format('CREATE OR REPLACE VIEW %s AS SELECT CASE ', _merge_view_name); -- create field to determine inherited table

		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			_sql_cmd := _sql_cmd || format('
				WHEN %1$I.%2$I IS NOT NULL THEN %1$L::%3$I.%4$s_type '
				, _child_table_name --1
				, (_child_table->>'pkey')::text --2
				, _destination_schema --3
				, _parent_table_name --4
			);
		END LOOP;
		_sql_cmd := _sql_cmd || format(' ELSE NULL::%2$I.%1$s_type END AS %1$s_type'
			, _parent_table_name --1
			, _destination_schema --2
		);
		-- add parent table columns
		_sql_cmd := _sql_cmd || format(', %1$I.%2$I, %3$s '
			, _parent_table_name --1
			, (_parent_table->>'pkey')::text --2
			, _parent_table_name || '.' || array_to_string(_parent_field_array, ', ' || _parent_table_name || '.') --3
		);
		-- additional columns if they exists
		FOR _additional_column IN SELECT json_object_keys(_parent_table->'merge_view'->'additional_columns') LOOP
			_sql_cmd := _sql_cmd || format(', %1$s AS %2$I'
				, _parent_table->'merge_view'->'additional_columns'->>_additional_column
				, _additional_column
			);
		END LOOP;
		-- add columns of children tables
		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list
			_sql_cmd := _sql_cmd || ', ' || _child_table_name || '.' || array_to_string(_child_field_array, ', '||_child_table_name||'.');
		END LOOP;
		-- from parent table
		_sql_cmd := _sql_cmd || format('
			FROM %1$s %2$I'
			, (_parent_table->>'table_name')::regclass
			, _parent_table_name
		);
		-- from children tables (LEFT JOIN)
		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			_sql_cmd := _sql_cmd || format('
				LEFT JOIN %1$s %2$I ON %3$I.%4$I = %2$s.%5$I '
				, (_child_table->>'table_name')::regclass --1
				, _child_table_name --2
				, _parent_table_name --3
				, (_parent_table->>'pkey')::text --4
				, (_child_table->>'pkey')::text --5
			);
		END LOOP;
		-- additional join if they exist
		_sql_cmd := _sql_cmd || format(' %s', _parent_table->'merge_view'->>'additional_join' );
		-- run it
		EXECUTE( _sql_cmd );



		-- insert function trigger for merge view
		_sql_cmd := format('
			CREATE OR REPLACE FUNCTION %1$s() RETURNS TRIGGER AS $$
			BEGIN
				INSERT INTO %2$s ( %3$I, %4$s ) VALUES ( %5$s, %6$s ) RETURNING %3$I INTO NEW.%3$I;
				CASE'
			, _destination_schema||'.ft_'||_merge_view_rootname||'_insert' --1
			, (_parent_table->>'table_name')::regclass --2
			, (_parent_table->>'pkey')::text --3
			, array_to_string(_parent_field_array, ', ') --4
			, _parent_table->>'pkey_nextval' --5
			, 'NEW.'||array_to_string(_parent_field_array, ', NEW.') --6
		);
		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list
			_sql_cmd := _sql_cmd || format('
				WHEN NEW.%1$I::%8$I.%1$I = %2$L THEN INSERT INTO %3$s ( %4$I, %5$s )VALUES (NEW.%6$I, %7$s );'
				, _parent_table_name || '_type' --1
				, _child_table_name::text --2
				, (_child_table->>'table_name')::regclass --3
				, (_child_table->>'pkey')::text --4
				, array_to_string(_child_field_array, ', ') --5
				, (_parent_table->>'pkey')::text --6
				, 'NEW.'||array_to_string(_child_field_array, ', NEW.') --7
				, _destination_schema --8
			);
		END LOOP;
		_sql_cmd := _sql_cmd || '
			END CASE;
			RETURN NEW;
			END;
			$$
			LANGUAGE plpgsql;';
		EXECUTE( _sql_cmd );
		EXECUTE format('
			CREATE TRIGGER %1$I
				  INSTEAD OF INSERT
				  ON %2$s
				  FOR EACH ROW
				  EXECUTE PROCEDURE %3$s();',
			'tr_'||_merge_view_rootname||'_insert', --1
			_merge_view_name::regclass, --2
			(_destination_schema||'.ft_'||_merge_view_rootname||'_insert')::regproc --3
		);



		-- update function trigger for merge view
		_sql_cmd := format('
			CREATE OR REPLACE FUNCTION %1$s() RETURNS TRIGGER AS $$
			BEGIN
			UPDATE %2$s SET %3$s WHERE %4$I = OLD.%4$I;
			/* Allow change type */
			IF OLD.%5$I <> NEW.%5$I::%6$I.%5$I THEN CASE'
			, _destination_schema||'.ft_'||_merge_view_rootname||'_update' --1
			, (_parent_table->>'table_name')::regclass --2
			, _parent_field_list --3
			, (_parent_table->>'pkey')::text --4
			, _parent_table_name || '_type' --5
			, _destination_schema --6
		);
		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			_sql_cmd := _sql_cmd || format('
				WHEN OLD.%1$I::%5$I.%1$I = %2$L THEN DELETE FROM %3$s WHERE %4$I = OLD.%4$I;'
				, _parent_table_name || '_type' --1
				, _child_table_name::text --2
				, (_child_table->>'table_name')::regclass --3
				, (_child_table->>'pkey')::text --4
				, _destination_schema --5
			);
		END LOOP;
		_sql_cmd := _sql_cmd || '
			END CASE;
			CASE';
		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			_sql_cmd := _sql_cmd || format('
				WHEN NEW.%1$I::%6$I.%1$I = %2$L THEN INSERT INTO %3$s (%4$I) VALUES (OLD.%5$I);'
				, _parent_table_name || '_type' --1
				, _child_table_name::text --2
				, (_child_table->>'table_name')::regclass --3
				, (_child_table->>'pkey')::text --4
				, (_parent_table->>'pkey')::text --5
				, _destination_schema --6
			);
		END LOOP;
		_sql_cmd := _sql_cmd || '
			END CASE;
			END IF;
			CASE ';
		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
			-- write list of fields for update command
			EXECUTE format(	$$ SELECT ARRAY( SELECT attname FROM pg_attribute WHERE attrelid = %1$L::regclass AND attnum > 0 ORDER BY attnum ASC ) $$, _child_table->>'table_name') INTO _child_field_array;
			_child_field_array := array_remove(_child_field_array, (_child_table->>'pkey')::text); -- remove pkey from field list
			SELECT array_to_string(f, ', ') -- create command of update rule for parent fiels
				FROM ( SELECT array_agg(f||' = NEW.'||f) AS f
				FROM unnest(_child_field_array)     AS f ) foo
				INTO _child_field_list;

			_sql_cmd := _sql_cmd || format('
				WHEN NEW.%1$I::%6$I.%1$I = %2$L THEN UPDATE %3$s SET %4$s WHERE %5$I = OLD.%5$I;'
				, _parent_table_name || '_type' --1
				, _child_table_name::text --2
				, (_child_table->>'table_name')::regclass --3
				, _child_field_list --4
				, (_child_table->>'pkey')::text --5
				, _destination_schema --6
				);
		END LOOP;
		_sql_cmd := _sql_cmd || '
			END CASE;
			RETURN NEW;
			END;
			$$
			LANGUAGE plpgsql;';
		EXECUTE( _sql_cmd );
		EXECUTE format('
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
		FOR _child_table_name IN SELECT json_object_keys(_parent_table->'inherited_by') LOOP
			_child_table := _parent_table->'inherited_by'->_child_table_name;
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
