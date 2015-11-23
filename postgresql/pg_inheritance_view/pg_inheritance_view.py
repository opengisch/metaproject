#!/usr/bin/env python


import psycopg2, psycopg2.extras
import yaml


class PGInheritanceView():
	def __init__(self, service, definition):

		self.conn = psycopg2.connect("service={0}".format(service))
		self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

		self.definition = yaml.load(definition)

		# add alias definition to children to have the same data structure than the top level (parent table)
		for child in self.definition['children']:
			self.definition['children'][child]['alias'] = child

		# defines if a item can be inserted in the parent table only (not any sub-type). Default: true.
		self.allow_parent_only = self.definition['allow_parent_only'] if 'allow_parent_only' in self.definition else True
		# defines if switching between sub-type is allowed in the merge_view. Default: false
		self.allow_type_change = False
		if 'merge_view' in self.definition and 'allow_type_change' in self.definition['merge_view']:
			self.allow_type_change = self.definition['merge_view']['allow_type_change']

	def columns(self, element):
		self.cur.execute("SELECT attname FROM pg_attribute WHERE attrelid = '{0}'::regclass AND attnum > 0 ORDER BY attnum ASC".format(element['table']))
		pg_fields = self.cur.fetchall()
		pg_fields = [field[0] for field in pg_fields]
		pg_fields.remove(element['pkey'])
		return pg_fields

	def column_alter_read(self, element, column):
		if 'alter' in element and column in element['alter'] and 'read' in element['alter'][column]:
			return element['alter'][column]['read']
		else:
			return None

	def column_alter_write(self, element, column):
		if 'alter' in element and column in element['alter'] and 'write' in element['alter'][column]:
			return element['alter'][column]['write']
		else:
			return None

	def column_remap(self, element, column):
		if 'remap' in element and column in element['remap']:
			return element['remap'][column]
		else:
			return None

	def join_view_name(self, child, schema_qualified=True):
		name =  '{0}.'.format(self.definition['schema']) if schema_qualified else ''
		name +='vw_{0}_{1}'.format(self.definition['alias'], child)
		return name

	def sql_all(self):
		sql = ''
		for child in self.definition['children']:
			sql += self.sql_join_view(child)
			sql += self.sql_join_insert_trigger(child)
			sql += self.sql_join_update_trigger(child)
			sql += self.sql_join_delete_trigger(child)
		sql += self.sql_merge_view()
		sql += self.sql_merge_insert_trigger()
		sql += self.sql_merge_update_trigger()
		sql += self.sql_merge_delete_trigger()
		return sql

	def sql_type(self):
		sql = "CREATE TYPE {0}.{1}_type AS ENUM ({2} {3} );\n\n".format(
			self.definition['schema'],
			self.definition['alias'],
			" '{0}',".format(self.definition['alias']) if self.allow_parent_only is True else "",
			', '.join(["'{0}'".format(child) for child in self.definition['children']])
			)
		return sql

	def sql_join_view(self, child):
		sql = "CREATE OR REPLACE VIEW {0} AS\n\tSELECT\n".format(self.join_view_name(child))

		sql += "\t\t{0}.{1}".format(self.definition['alias'], self.definition['pkey'])

		# parent columns
		for element in (self.definition, self.definition['children'][child]):
			for col in self.columns(element):
				col_alter_read = self.column_alter_read(element, col)
				col_remap = self.column_remap(element, col)
				sql += "\n\t\t, "
				if col_alter_read:
					sql += "{0}({1}.{2})".format(col_alter_read, element['alias'], col)
					if not col_remap:
						sql += " AS {0}".format(col)
				else:
					sql += "{0}.{1}".format(element['alias'],col)
				if col_remap:
					sql += " AS {0}".format(col_remap)

		# from tables
		sql += "\n\tFROM {0} {1}\n\tINNER JOIN {2} {3}\n\t\tON {1}.{4} = {3}.{5};\n\n".format(
			self.definition['children'][child]['table'],
			self.definition['children'][child]['alias'],
			self.definition['table'],
			self.definition['alias'],
			self.definition['children'][child]['pkey'],
			self.definition['pkey']
			)

		return sql


	def sql_join_insert_trigger(self, child):
		parent_columns = self.columns(self.definition)
		child_columns = self.columns(self.definition['children'][child])

		functrigger = "{0}.ft_{1}_{2}_insert".format(self.definition['schema'],	self.definition['alias'], child)
		trigger = "tr_{1}_{2}_insert".format(self.definition['schema'],	self.definition['alias'], child)

		sql =  "CREATE OR REPLACE FUNCTION {0}()\n".format(functrigger)
		sql += "\tRETURNS trigger AS\n"
		sql += "\t$$\n"
		sql += "\tBEGIN\n"

		# insert into parent
		if 'pkey_value_create_entry' in self.definition and self.definition['pkey_value_create_entry']:
			# Allow to use function to create entry
			# the function is defined by pkey_value
			# if exists, pkey_value is triggered and will return an ID
			# then, this feature is updated
			sql += "\t\t-- The function creates or gets a parent row.\n"
			sql += "\t\tNEW.{0} := {1};\n".format(
				self.definition['pkey'],
				self.definition['pkey_value'])
			sql += "\t\t-- If it previously existed with another subtype, it should raise an exception\n"
			sql += "\t\tIF (SELECT _oid IS NOT NULL FROM \n\t\t\t(\n\t\t\t\t{0}\n\t\t\t) AS foo WHERE _oid = NEW.{1}\n\t\t) THEN\n".format(
				' UNION\n\t\t\t\t'.join(
					['SELECT {0} AS _oid FROM {1}'.format(
						self.definition['children'][child]['pkey'],
						self.definition['children'][child]['table']
					) for child in self.definition['children']]
				),
				self.definition['pkey']
			)
			sql += "\t\t\tRAISE EXCEPTION 'Cannot insert {0} as {1} since it already has another subtype. ID: %%', NEW.{2};\n".format(
				self.definition['alias'],
				child,
				self.definition['pkey']
				)
			sql += "\t\tEND IF;\n"
			sql += "\t\t-- Now update the existing or created feature in parent table\n"
			sql += "\t\tUPDATE {0} SET\n".format(self.definition['table'])
			for col in parent_columns:
				col_alter_write = self.column_alter_write(self.definition, col)
				col_remap = self.column_remap(self.definition, col)

				sql += "\t\t\t\t{0} = {1}{2}{3},\n".format(
					col,
					'{0}('.format(col_alter_write) if col_alter_write else '',
					col_remap if col_remap else col,
					')' if col_alter_write else ''
					)
			sql = sql[:-2]+'\n'
			sql += "\t\t\tWHERE {0} = NEW.{0};\n".format(self.definition['pkey'])
		else:
			sql += "\t\tINSERT INTO {0} (\n\t\t\t{1}\n\t\t\t{2}\n\t\t) VALUES (\n\t\t\t{3} ".format(
				self.definition['table'],
				self.definition['pkey'],
				'\n\t\t\t'.join([", {0}".format(col) for col in parent_columns]),
				self.definition['pkey_value']
				)
			for col in parent_columns:
				col_alter_write = self.column_alter_write(self.definition, col)
				col_remap = self.column_remap(self.definition, col)
				if not col_remap:
					col_remap = col
				sql += "\n\t\t\t, "
				if col_alter_write:
					sql += "{0}( ".format(col_alter_write)
				sql += 'NEW.{0}'.format(col_remap)
				if col_alter_write:
					sql += " )"
			sql += "\n\t\t) RETURNING {0} INTO NEW.{0};\n".format(self.definition['pkey'])

		# insert into child
		sql += "\n\t\tINSERT INTO {0} (\n\t\t\t{1}\n\t\t\t{2}\n\t\t) VALUES (\n\t\t\tNEW.{3} ".format(
			self.definition['children'][child]['table'],
			self.definition['children'][child]['pkey'],
			'\n\t\t\t'.join([", {0}".format(col) for col in child_columns]),
			self.definition['pkey']
			)
		for col in child_columns:
			col_alter_write = self.column_alter_write(self.definition['children'][child], col)
			col_remap = self.column_remap(self.definition['children'][child], col)
			if not col_remap:
					col_remap = col
			sql += "\n\t\t\t, "
			if col_alter_write:
				sql += "{0}( ".format(col_alter_write)
			sql += 'NEW.{0}'.format(col_remap)
			if col_alter_write:
				sql += " )"
		sql += "\n\t\t);\n"

		# end trigger function
		sql += "\t\tRETURN NEW;\n"
		sql += "\tEND;\n"
		sql += "\t$$\n"
		sql += "\tLANGUAGE plpgsql;\n\n"

		# create trigger
		sql += "DROP TRIGGER IF EXISTS {0} ON {1};\n".format(trigger, self.join_view_name(child))
		sql += "CREATE TRIGGER {0}\n".format(trigger)
		sql += "\tINSTEAD OF INSERT\n"
		sql += "\tON {0}\n".format(self.join_view_name(child))
		sql += "\tFOR EACH ROW\n"
		sql += "\tEXECUTE PROCEDURE {0}();\n\n".format(functrigger)

		return sql

	def sql_join_update_trigger(self, child):
		parent_columns = self.columns(self.definition)
		child_columns = self.columns(self.definition['children'][child])

		functrigger = "{0}.ft_{1}_{2}_update".format(self.definition['schema'],	self.definition['alias'], child)
		trigger = "tr_{1}_{2}_update".format(self.definition['schema'],	self.definition['alias'], child)

		sql =  "\nCREATE OR REPLACE FUNCTION {0}()".format(functrigger)
		sql += "\n\tRETURNS trigger AS"
		sql += "\n\t$$"
		sql += "\n\tBEGIN"

		for element in (self.definition, self.definition['children'][child]):
			cols = self.columns(element)
			if len(cols) > 0:
				sql += "\n\tUPDATE {0} SET".format(element['table'])
				for col in cols:
					col_alter_write = self.column_alter_write(element, col)
					col_remap = self.column_remap(element, col)
					if not col_remap:
						col_remap = col

					sql += "\n\t\t\t{0} = {1}{2}{3},".format(
						col,
						'{0}('.format(col_alter_write) if col_alter_write else '',
						col_remap,
						')' if col_alter_write else ''
						)
				sql = sql[:-1] # extra comma
				sql += "\n\t\tWHERE {0} = NEW.{0};\n".format(element['pkey'])

		sql += "\t\tRETURN NEW;\n"
		sql += "\tEND;\n"
		sql += "\t$$\n"
		sql += "\tLANGUAGE plpgsql;\n\n"

		# create trigger
		sql += "DROP TRIGGER IF EXISTS {0} ON {1};\n".format(trigger, self.join_view_name(child))
		sql += "CREATE TRIGGER {0}\n".format(trigger)
		sql += "\tINSTEAD OF UPDATE\n"
		sql += "\tON {0}\n".format(self.join_view_name(child))
		sql += "\tFOR EACH ROW\n"
		sql += "\tEXECUTE PROCEDURE {0}();\n\n".format(functrigger)

		return sql

	def sql_join_delete_trigger(self, child):

		functrigger = "{0}.ft_{1}_{2}_delete".format(self.definition['schema'],	self.definition['alias'], child)
		trigger = "tr_{1}_{2}_delete".format(self.definition['schema'],	self.definition['alias'], child)

		sql =  "\nCREATE OR REPLACE FUNCTION {0}()".format(functrigger)
		sql += "\n\tRETURNS trigger AS"
		sql += "\n\t$$"
		sql += "\n\tBEGIN"

		if "custom_delete" in self.definition['children'][child]:
			sql += "\n\t\t{0};".format(self.definition['children'][child]['custom_delete'])
		else:
			sql += "\n\t\tDELETE FROM {0} WHERE {1} = OLD.{1};".format(self.definition['children'][child]['table'], self.definition['children'][child]['pkey'])
		if "custom_delete" in self.definition:
			sql += "\n\t\t{0};".format(self.definition['custom_delete'])
		else:
			sql += "\n\t\tDELETE FROM {0} WHERE {1} = OLD.{1};".format(self.definition['table'], self.definition['pkey'])

		sql += "\n\t\tRETURN NULL;\n"
		sql += "\tEND;\n"
		sql += "\t$$\n"
		sql += "\tLANGUAGE plpgsql;\n\n"

		# create trigger
		sql += "DROP TRIGGER IF EXISTS {0} ON {1};\n".format(trigger, self.join_view_name(child))
		sql += "CREATE TRIGGER {0}\n".format(trigger)
		sql += "\tINSTEAD OF UPDATE\n"
		sql += "\tON {0}\n".format(self.join_view_name(child))
		sql += "\tFOR EACH ROW\n"
		sql += "\tEXECUTE PROCEDURE {0}();\n\n".format(functrigger)

		return sql

	def sql_merge_view(self):
		if 'merge_view' not in self.definition:
			return ''

		sql = self.sql_type()

		sql += "CREATE OR REPLACE VIEW {0}.{1} AS\n\tSELECT\n\t\tCASE\n".format(self.definition['schema'],self.definition['merge_view']['name'])
		for child in self.definition['children']:
			sql += "\t\t\tWHEN {0}.{1} IS NOT NULL THEN '{0}'::{2}.{3}_type\n".format(
				child,
				self.definition['children'][child]['pkey'],
				self.definition['schema'],
				self.definition['alias']
				)
		sql += "\t\t\tELSE '{0}'::{1}.{0}_type\n".format(
			self.definition['alias'],
			self.definition['schema']
			)
		sql += "\t\tEND AS {0}_type,\n".format(self.definition['alias'])
		sql += "\t\t{0}.{1}".format(self.definition['alias'],self.definition['pkey'])

		# parent columns
		parent_columns = self.columns (self.definition)
		for col in parent_columns:
			col_alter_read = self.column_alter_read(self.definition, col)
			col_remap = self.column_remap(self.definition, col)
			sql += "\n\t\t, "
			if col_alter_read:
				sql += "{0}({1}.{2})".format(col_alter_read, self.definition['alias'], col)
				if not col_remap:
					sql += " AS {0}".format(col)
			else:
				sql += "{0}.{1}".format(self.definition['alias'],col)
			if col_remap:
				sql += " AS {0}".format(col_remap)

		# additional columns
		if 'additional_columns' in self.definition['merge_view']:
			for col in self.definition['merge_view']['additional_columns']:
				sql += "\n\t\t, {0} AS {1}".format(self.definition['merge_view']['additional_columns'][col], col)

		# merge columns
		if 'merge_columns' in self.definition['merge_view']:
			for column_alias in self.definition['merge_view']['merge_columns']:
				sql += "\n\t\t, CASE"
				for table_alias in  self.definition['merge_view']['merge_columns'][column_alias]:
					sql += "\n\t\t\tWHEN {0}.{1} IS NOT NULL THEN {0}.{2}".format(
						table_alias,
						self.definition['children'][table_alias]['pkey'],
						self.definition['merge_view']['merge_columns'][column_alias][table_alias]
						)
				sql += "\n\t\t\tELSE NULL"
				sql += "\n\t\tEND AS {0}".format(column_alias)

		# children tables
		for child in self.definition['children']:
			child_columns = self.columns(self.definition['children'][child])
			# remove merged columns
			if 'merge_columns' in self.definition['merge_view']:
				for column_alias in self.definition['merge_view']['merge_columns']:
					for table_alias in self.definition['merge_view']['merge_columns'][column_alias]:
						if table_alias == child:
							child_columns.remove(self.definition['merge_view']['merge_columns'][column_alias][child])
			# add columns
			for col in child_columns:
				col_alter_read = self.column_alter_read(self.definition['children'][child], col)
				col_remap = self.column_remap(self.definition['children'][child], col)
				sql += "\n\t\t, "
				if col_alter_read:
					sql += "{0}({1}.{2})".format(col_alter_read, child, col)
					if not col_remap:
						sql += " AS {0}".format(col)
				else:
					sql += "{0}.{1}".format(child,col)
				if col_remap:
					sql += " AS {0}".format(col_remap)

		# from
		sql += "\n\tFROM {0} {1}".format(self.definition['table'], self.definition['alias'])
		for child in self.definition['children']:
			sql += "\n\t\tLEFT JOIN {0} {1} ON {2}.{3} = {1}.{4}".format(
				self.definition['children'][child]['table'],
				child,
				self.definition['alias'],
				self.definition['pkey'],
				self.definition['children'][child]['pkey']
				)
		if 'additional_join' in self.definition['merge_view']:
			sql += "\n\t\t{0}".format(self.definition['merge_view']['additional_join'])
		sql += ";\n\n"
		return sql


	def sql_merge_insert_trigger(self):
		if 'merge_view' not in self.definition:
			return ''

		parent_columns = self.columns(self.definition)

		functrigger = "{0}.ft_{1}_insert".format(self.definition['schema'], self.definition['merge_view']['name'])
		trigger = "tr_{0}_insert".format(self.definition['merge_view']['name'])

		sql =  "CREATE OR REPLACE FUNCTION {0}()\n".format(functrigger)
		sql += "\tRETURNS trigger AS\n"
		sql += "\t$$\n"
		sql += "\tBEGIN\n"

		# insert into parent
		if 'pkey_value_create_entry' in self.definition and self.definition['pkey_value_create_entry']:
			# Allow to use function to create entry
			# the function is defined by pkey_value
			# if exists, pkey_value is triggered and will return an ID
			# then, this feature is updated
			sql += "\t\t-- The function creates or gets a parent row.\n"
			sql += "\t\tNEW.{0} := {1};\n".format(
				self.definition['pkey'],
				self.definition['pkey_value'])
			sql += "\t\t-- If it previously existed with another subtype, it should raise an exception\n"
			sql += "\t\tIF (SELECT _oid IS NOT NULL FROM \n\t\t\t(\n\t\t\t\t{0}\n\t\t\t) AS foo WHERE _oid = NEW.{1}\n\t\t) THEN\n".format(
				' UNION\n\t\t\t\t'.join(
					['SELECT {0} AS _oid FROM {1}'.format(
						self.definition['children'][child]['pkey'],
						self.definition['children'][child]['table']
					) for child in self.definition['children']]
				),
				self.definition['pkey']
			)
			sql += "\t\t\tRAISE EXCEPTION 'Cannot insert {0} as {1} since it already has another subtype. ID: %%', NEW.{2};\n".format(
				self.definition['alias'],
				child,
				self.definition['pkey']
				)
			sql += "\t\tEND IF;\n"
			sql += "\t\t-- Now update the existing or created feature in parent table\n"
			sql += "\t\tUPDATE {0} SET\n".format(self.definition['table'])
			for col in parent_columns:
				col_alter_write = self.column_alter_write(self.definition, col)
				col_remap = self.column_remap(self.definition, col)
				if not col_remap:
					col_remap = col

				sql += "\t\t\t\t{0} = {1}{2}{3},\n".format(
					col,
					'{0}('.format(col_alter_write) if col_alter_write else '',
					col_remap,
					')' if col_alter_write else ''
					)
			sql = sql[:-2]+'\n'
			sql += "\t\t\tWHERE {0} = NEW.{0};\n".format(self.definition['pkey'])
		# standard insert
		else:
			sql += "\t\tINSERT INTO {0} (\n\t\t\t{1}\n\t\t\t{2}\n\t\t) VALUES (\n\t\t\t{3} ".format(
				self.definition['table'],
				self.definition['pkey'],
				'\n\t\t\t'.join([", {0}".format(col) for col in parent_columns]),
				self.definition['pkey_value']
				)
			for col in parent_columns:
				col_alter_write = self.column_alter_write(self.definition, col)
				col_remap = self.column_remap(self.definition, col)
				if not col_remap:
					col_remap = col
				sql += "\n\t\t\t, "
				if col_alter_write:
					sql += "{0}( ".format(col_alter_write)
				sql += 'NEW.{0}'.format(col_remap)
				if col_alter_write:
					sql += " )"
			sql += "\n\t\t) RETURNING {0} INTO NEW.{0};\n".format(self.definition['pkey'])

		# insert into children
		sql += "\n\tCASE"
		for child in self.definition['children']:
			child_columns = self.columns(self.definition['children'][child])

			sql += "\n\t\tWHEN NEW.{0}_type::{1}.{0}_type = '{2}'::{1}.{0}_type\n\t\t\tTHEN INSERT INTO {3} (\n\t\t\t\t{4} {5}\n\t\t\t) VALUES (\n\t\t\t\tNEW.{6}".format(
				self.definition['alias'],
				self.definition['schema'],
				child,
				self.definition['children'][child]['table'],
				self.definition['children'][child]['pkey'],
				''.join(["\n\t\t\t\t, {0}".format(col) for col in child_columns]),
				self.definition['pkey']
				)

			for col in child_columns:
				col_alter_write = self.column_alter_write(self.definition['children'][child], col)
				col_remap = self.column_remap(self.definition['children'][child], col)
				if not col_remap:
					col_remap = col
					# replace remapped column by merged column alias if exists
					if 'merge_columns' in self.definition['merge_view']:
						for column_alias in self.definition['merge_view']['merge_columns']:
							for table_alias in self.definition['merge_view']['merge_columns'][column_alias]:
								if table_alias == child:
									if col_remap == self.definition['merge_view']['merge_columns'][column_alias][table_alias]:
										col_remap = column_alias
				sql += "\n\t\t\t\t, "
				if col_alter_write:
					sql += "{0}( ".format(col_alter_write)
				sql += 'NEW.{0}'.format(col_remap)
				if col_alter_write:
					sql += " )"
			sql += "\n\t\t);\n"
		sql += "\n\t END CASE;\n"

		# end trigger function
		sql += "\t\tRETURN NEW;\n"
		sql += "\tEND;\n"
		sql += "\t$$\n"
		sql += "\tLANGUAGE plpgsql;\n\n"

		# create trigger
		sql += "DROP TRIGGER IF EXISTS {0} ON {1}.{2};\n".format(trigger, self.definition['schema'], self.definition['merge_view']['name'])
		sql += "CREATE TRIGGER {0}\n".format(trigger)
		sql += "\tINSTEAD OF INSERT\n"
		sql += "\tON {0}.{1}\n".format(self.definition['schema'], self.definition['merge_view']['name'])
		sql += "\tFOR EACH ROW\n"
		sql += "\tEXECUTE PROCEDURE {0}();\n\n".format(functrigger)

		return sql

	def sql_merge_update_trigger(self):
		if 'merge_view' not in self.definition:
			return ''

		parent_columns = self.columns(self.definition)

		functrigger = "{0}.ft_{1}_update".format(self.definition['schema'], self.definition['merge_view']['name'])
		trigger = "tr_{0}_update".format(self.definition['merge_view']['name'])


		sql =  "\nCREATE OR REPLACE FUNCTION {0}()".format(functrigger)
		sql += "\n\tRETURNS trigger AS"
		sql += "\n\t$$"
		sql += "\n\tBEGIN"

		# parent columns
		cols = self.columns(self.definition)
		if len(cols) > 0:
			sql += "\n\tUPDATE {0} SET".format(self.definition['table'])
			for col in cols:
				col_alter_write = self.column_alter_write(self.definition, col)
				col_remap = self.column_remap(self.definition, col)
				if not col_remap:
					col_remap = col

				sql += "\n\t\t\t{0} = {1}{2}{3},".format(
					col,
					'{0}('.format(col_alter_write) if col_alter_write else '',
					col_remap,
					')' if col_alter_write else ''
					)
			sql = sql[:-1] # extra comma
			sql += "\n\t\tWHERE {0} = NEW.{0};".format(self.definition['pkey'])

		# do not allow parent only insert
		if not self.allow_parent_only:
			sql += "\n\tIF NEW.{0}_type IS NULL THEN".format(self.definition['alias'])
			sql += "\n\t\tRAISE EXCEPTION 'Insert on {0} only is not allowed.' USING HINT = 'It must have a sub-type.';".format(self.definition['alias'])
			sql += "\n\tEND IF;"

		# detect if type has changed
		sql += "\n\t-- detect if type has changed"
		sql += "\n\tIF OLD.{0}_type <> NEW.{0}_type::{1}.{0}_type THEN".format(self.definition['alias'], self.definition['schema'])
		# allow type change
		if self.allow_type_change:
			sql += "\n\t\t-- delete old sub type"
			sql += "\n\t\tCASE"
			for child in self.definition['children']:
				sql += "\n\t\t\tWHEN OLD.{0}_type::{1}.{0}_type = '{2}'::{1}.{0}_type".format(self.definition['alias'], self.definition['schema'], child)
				sql += "\n\t\t\t\tTHEN DELETE FROM {0} WHERE {1} = OLD.{1};".format(self.definition['children'][child]['table'], self.definition['children'][child]['pkey'])
			sql += "\n\t\tEND CASE;"
			sql += "\n\t\t-- insert new sub type"
			sql += "\n\t\tCASE"
			for child in self.definition['children']:
				child_columns = self.columns(self.definition['children'][child])
				sql += "\n\t\t\tWHEN NEW.{0}_type::{1}.{0}_type = '{2}'::{1}.{0}_type".format(self.definition['alias'], self.definition['schema'], child)
				sql += "\n\t\t\t\tTHEN INSERT INTO {0} (\n\t\t\t\t\t\t{1} {2} \n\t\t\t\t\t) VALUES (\n\t\t\t\t\t\tOLD.{3}".format(
					self.definition['children'][child]['table'],
					self.definition['children'][child]['pkey'],
					''.join(["\n\t\t\t\t\t\t, {0}".format(col) for col in child_columns]),
					self.definition['pkey']
					)
				for col in child_columns:
					col_alter_write = self.column_alter_write(self.definition['children'][child], col)
					col_remap = self.column_remap(self.definition['children'][child], col)
					if not col_remap:
						col_remap = col
						# replace remapped column by merged column alias if exists
						if 'merge_columns' in self.definition['merge_view']:
							for column_alias in self.definition['merge_view']['merge_columns']:
								for table_alias in self.definition['merge_view']['merge_columns'][column_alias]:
									if table_alias == child:
										if col_remap == self.definition['merge_view']['merge_columns'][column_alias][table_alias]:
											col_remap = column_alias
					sql += "\n\t\t\t\t\t\t, "
					if col_alter_write:
						sql += "{0}( ".format(col_alter_write)
					sql += 'NEW.{0}'.format(col_remap)
					if col_alter_write:
						sql += " )"
				sql += "\n\t\t\t\t\t);"
			sql += "\n\t\tEND CASE;"
			sql += "\n\t\t-- return now as child has been updated"
			sql += "\n\t\tRETURN NEW;"
		# forbid type change
		else:
			sql += "\n\t\tRAISE EXCEPTION 'Type change not allowed for {0}'".format(self.definition['alias'])
			sql += "\n\t\t\tUSING HINT = 'You cannot switch from ' || OLD.{0}_type || ' to ' || NEW.{0}_type; ".format(self.definition['alias'])
		sql += "\n\tEND IF;"

		# update child
		sql += "\n\tCASE"
		for child in self.definition['children']:
			child_columns = self.columns(self.definition['children'][child])
			sql += "\n\tWHEN NEW.{0}_type::{1}.{0}_type = '{2}'::{1}.{0}_type\n\t\tTHEN UPDATE {3} SET \n\t\t\t{4} = {5}".format(
				self.definition['alias'],
				self.definition['schema'],
				child,
				self.definition['children'][child]['table'],
				self.definition['children'][child]['pkey'],
				self.definition['pkey']
				)
			for col in child_columns:
				col_alter_write = self.column_alter_write(self.definition['children'][child], col)
				col_remap = self.column_remap(self.definition['children'][child], col)
				if not col_remap:
					col_remap = col
					# replace remapped column by merged column alias if exists
					if 'merge_columns' in self.definition['merge_view']:
						for column_alias in self.definition['merge_view']['merge_columns']:
							for table_alias in self.definition['merge_view']['merge_columns'][column_alias]:
								if table_alias == child:
									if col_remap == self.definition['merge_view']['merge_columns'][column_alias][table_alias]:
										col_remap = column_alias
				sql += "\n\t\t\t, {0} = ".format(col)
				if col_alter_write:
					sql += "{0}( ".format(col_alter_write)
				sql += 'NEW.{0}'.format(col_remap)
				if col_alter_write:
					sql += " )"
			sql += ";"
		sql += "\n\tEND CASE;\n"


		sql += "\n\tRETURN NEW;"
		sql += "\n\tEND;"
		sql += "\n\t$$"
		sql += "\n\tLANGUAGE plpgsql;\n"

		# update trigger
		sql += "DROP TRIGGER IF EXISTS {0} ON {0}.{1};\n".format(trigger, self.definition['schema'], self.definition['merge_view']['name'])
		sql += "CREATE TRIGGER {0}\n".format(trigger)
		sql += "\tINSTEAD OF UPDATE\n"
		sql += "\tON {0}.{1}\n".format(self.definition['schema'], self.definition['merge_view']['name'])
		sql += "\tFOR EACH ROW\n"
		sql += "\tEXECUTE PROCEDURE {0}();\n\n".format(functrigger)

		return sql

	def sql_merge_delete_trigger(self):
		if 'merge_view' not in self.definition:
			return ''

		functrigger = "{0}.ft_{1}_delete".format(self.definition['schema'], self.definition['merge_view']['name'])
		trigger = "tr_{0}_delete".format(self.definition['merge_view']['name'])

		sql = ''

		sql =  "\nCREATE OR REPLACE FUNCTION {0}()".format(functrigger)
		sql += "\n\tRETURNS trigger AS"
		sql += "\n\t$$"
		sql += "\n\tBEGIN"

		sql += "\n\tCASE"
		for child in self.definition['children']:
			sql += "\n\t\tWHEN OLD.{0}_type::{1}.{0}_type = '{2}'::{1}.{0}_type THEN".format(
				self.definition['alias'],
				self.definition['schema'],
				child
				)
			if "custom_delete" in self.definition['children'][child]:
				sql += "\n\t\t\t{0};".format(self.definition['children'][child]['custom_delete'])
			else:
				sql += "\n\t\t\tDELETE FROM {0} WHERE {1} = OLD.{1};".format(self.definition['children'][child]['table'], self.definition['children'][child]['pkey'])
		sql += "\n\tEND CASE;"
		if "custom_delete" in self.definition:
			sql += "\n\t{0};".format(self.definition['custom_delete'])
		else:
			sql += "\n\tDELETE FROM {0} WHERE {1} = OLD.{1};".format(self.definition['table'], self.definition['pkey'])
		sql += "\n\tRETURN NULL;\n"
		sql += "\tEND;\n"
		sql += "\t$$\n"
		sql += "\tLANGUAGE plpgsql;\n\n"

		# delete trigger
		sql += "DROP TRIGGER IF EXISTS {0} ON {1}.{2};\n".format(trigger, self.definition['schema'], self.definition['merge_view']['name'])
		sql += "CREATE TRIGGER {0}\n".format(trigger)
		sql += "\tINSTEAD OF DELETE\n"
		sql += "\tON {0}.{1}\n".format(self.definition['schema'], self.definition['merge_view']['name'])
		sql += "\tFOR EACH ROW\n"
		sql += "\tEXECUTE PROCEDURE {0}();\n\n".format(functrigger)

		return sql
