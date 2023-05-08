import os
from glob import glob
from lark import Lark, Transformer, exceptions
from berkeleydb import db


prompt_msg = "MY_DB> "


class MyTransformer(Transformer):
    def command(self, items):
        # When query is exit; or EXIT;
        if hasattr(items[0], 'type'):
            exit(0)
        # When query is not exit; or EXIT;
        else:
            return items[0]
    

    def create_table_query(self, items):

        # Create table schema database file
        table_name = items[2].children[0].lower()
        table_schema_path = 'DB/'+table_name+'_schema.db'

        if os.path.exists(table_schema_path):
            print(f"{prompt_msg}Create table has failed: table with the same name already exists") # TableExistenceError
            return
        
        metaDB = db.DB()
        metaDB.open(table_schema_path, dbtype=db.DB_HASH, flags=db.DB_CREATE)

        # Iterate column definitions to find column name, column type and whether column has not null constraint
        # Then put column name as key and column type as value into db
        column_definition_iter = items[3].find_data("column_definition")
        column_names, not_null = [], []
        for column_definition in column_definition_iter:
            column_name = column_definition.children[0].children[0].lower()
            column_type = column_definition.children[1].children[0].lower()
            
            if column_type == "char":
                char_len = column_definition.children[1].children[2]
                if int(char_len) < 1:
                    print(f"{prompt_msg}Char length should be over 0") # CharLengthError
                    os.remove(table_schema_path)
                    return
                column_type += char_len
            try:
                metaDB.put(column_name.encode(), column_type.encode(), flags=db.DB_NOOVERWRITE)
            except:
                print(f"{prompt_msg}Create table has failed: column definition is duplicated") # DuplicateColumnDefError
                os.remove(table_schema_path)
                return
            
            column_names.append(column_name)

            column_is_not_null = column_definition.children[2]
            if column_is_not_null:
                not_null.append(column_name)

        # Put 'column_names' as key and column names separated by 'COLUMN' as value into db
        metaDB.put('column_names'.encode(), 'COLUMN'.join(column_names).encode())
        
        # Put 'not_null' as key and not null column names separated by 'COLUMN' as value into db
        metaDB.put('not_null'.encode(), 'COLUMN'.join(not_null).encode())

        # Iterate primary key constraints to find primary key column names
        primary_key_constraint_iter = items[3].find_data("primary_key_constraint")
        primary_key = []
        is_duplicate_primary_key = False
        for primary_key_constraint in primary_key_constraint_iter:
            if is_duplicate_primary_key:
                print(f"{prompt_msg}Create table has failed: primary key definition is duplicated") # DuplicatePrimaryKeyDefError
                os.remove(table_schema_path)
                return
            primary_column_iter = primary_key_constraint.find_data("column_name")
            for primary_column in primary_column_iter:
                primary_column_name = primary_column.children[0].lower()
                if not metaDB.exists(primary_column_name.encode()):
                    print(f"{prompt_msg}Create table has failed: {primary_column_name} does not exist in column definition") # NonExistingColumnDefError
                    os.remove(table_schema_path)
                    return
                primary_key.append(primary_column_name)
            is_duplicate_primary_key = True
        
        # Put 'primary_key' as key and primary key column names separated by 'COLUMN' as value into db
        metaDB.put('primary_key'.encode(), 'COLUMN'.join(primary_key).encode())

        # Put 'reference_count' as key and 0 as value
        metaDB.put('reference_count'.encode(), '0'.encode())
        
        # Iterate foreign key constraints to find foreign key column names and their references
        referential_constraint_iter = items[3].find_data("referential_constraint")
        foreign_keys, referenced_tables = [], []
        for referential_constraint in referential_constraint_iter:
            foreign_key, referenced_key = [], []

            referencing_column_iter = referential_constraint.children[2].find_data("column_name")
            for referencing_column in referencing_column_iter:
                referencing_column_name = referencing_column.children[0].lower()
                if not metaDB.exists(referencing_column_name.encode()):
                    print(f"{prompt_msg}Create table has failed: {referencing_column_name} does not exist in column definition") # NonExistingColumnDefError
                    os.remove(table_schema_path)
                    return
                foreign_key.append(referencing_column_name)

            referenced_table_name = referential_constraint.children[4].children[0].lower()
            if table_name == referenced_table_name:
                print(f"{prompt_msg}Create table has failed: cannot reference itself")
                os.remove(table_schema_path)
                return
            referenced_table_schema_path = 'DB/'+referenced_table_name+'_schema.db'
            referenced_tables.append(referenced_table_schema_path)

            referenced_column_iter = referential_constraint.children[5].find_data("column_name")
            for referenced_column in referenced_column_iter:
                referenced_column_name = referenced_column.children[0].lower()
                referenced_key.append(referenced_column_name)
            
            referencedDB = db.DB()
            try:
                referencedDB.open(referenced_table_schema_path, dbtype=db.DB_HASH)
            except:
                print(f"{prompt_msg}Create table has failed: foreign key references non existing table") # ReferenceTableExistenceError
                os.remove(table_schema_path)
                return

            # Iterate foreign key and referenced key to compare correctness
            for referencing_column_name, referenced_column_name in zip(foreign_key, referenced_key):
                if not referencedDB.exists(referenced_column_name.encode()):
                    print(f"{prompt_msg}Create table has failed: foreign key references non existing column") # ReferenceColumnExistenceError
                    os.remove(table_schema_path)
                    referencedDB.close()
                    return
                
                referenced_table_primary_key = referencedDB.get('primary_key'.encode())
                referenced_table_primary_key_list = referenced_table_primary_key.decode().split('COLUMN')

                referenced_column_is_non_primary = False
                if referenced_table_primary_key is None:
                    referenced_column_is_non_primary = True
                else:
                    if referenced_column_name not in referenced_table_primary_key_list:
                        referenced_column_is_non_primary = True
                    else:
                        referenced_table_primary_key_list.remove(referenced_column_name)
                if referenced_column_is_non_primary:
                    print(f"{prompt_msg}Create table has failed: foreign key references non primary key column") # ReferenceNonPrimaryKeyError
                    os.remove(table_schema_path)
                    referencedDB.close()
                    return
                
                referencing_column_type = metaDB.get(referencing_column_name.encode()).decode()
                referenced_column_type = referencedDB.get(referenced_column_name.encode()).decode()
                if referencing_column_type != referenced_column_type:
                    print(f"{prompt_msg}Create table has failed: foreign key references wrong type") # ReferenceTypeError
                    os.remove(table_schema_path)
                    referencedDB.close()
                    return
            
            # When foreign key references not all primary key
            if referenced_table_primary_key_list:
                print(f"{prompt_msg}Create table has failed: foreign key references non primary key column") # ReferenceNonPrimaryKeyError
                os.remove(table_schema_path)
                referencedDB.close()
                return
            
            #If this foreign key has no error
            foreign_keys.append('COLUMN'.join(foreign_key)+'REFERENCE'+referenced_table_name+'REFERENCE'+'COLUMN'.join(referenced_key))
            referencedDB.close()
            
        # If all foreign keys have no error
        metaDB.put('foreign_key'.encode(), 'FOREIGN'.join(foreign_keys).encode())
        if foreign_keys:
            for referenced_table_schema_path in referenced_tables:
                referencedDB = db.DB()
                referencedDB.open(referenced_table_schema_path, dbtype=db.DB_HASH)
                reference_count = str(int(referencedDB.get('reference_count'.encode()).decode()) + 1)
                referencedDB.put('reference_count'.encode(), reference_count.encode())
                referencedDB.close()
                
        # Create table success
        print(f"{prompt_msg}\'{table_name}\' table is created")
        metaDB.close()


    def drop_table_query(self, items):
        table_name = items[2].children[0].lower()
        table_path = 'DB/'+table_name+'.db'
        table_schema_path = 'DB/'+table_name+'_schema.db' 

        if not os.path.exists(table_schema_path):
            print(f"{prompt_msg}No such table") # NoSuchTable
            return
        
        metaDB = db.DB()
        metaDB.open(table_schema_path, dbtype=db.DB_HASH)

        if metaDB.get('reference_count'.encode()).decode() != '0':
            print(f"{prompt_msg}Drop table has failed: '{table_name}' is referenced by other table") # DropReferencedTableError
            metaDB.close()
            return
        
        # If drop has no errors, decrease reference_count by 1 in referenced table
        if metaDB.get('foreign_key'.encode()).decode():
            foreign_keys = metaDB.get('foreign_key'.encode()).decode().split('FOREIGN')
            for foreign_key in foreign_keys:
                referenced_table_name = foreign_key.split('REFERENCE')[1]
                referenced_table_schema_path = 'DB/'+referenced_table_name+'_schema.db'
                referencedDB = db.DB()
                referencedDB.open(referenced_table_schema_path, dbtype=db.DB_HASH)
                reference_count = str(int(referencedDB.get('reference_count'.encode()).decode()) - 1)
                referencedDB.put('reference_count'.encode(), reference_count.encode())
                
        # Drop success
        os.remove(table_schema_path)
        if os.path.exists(table_path):
            os.remove(table_path)
        print(f"{prompt_msg}'{table_name}' table is dropped")
        
    
    def explain_query(self, items):
        self.desc_query(items)
    

    def describe_query(self, items):
        self.desc_query(items)


    def desc_query(self, items):
        table_name = items[1].children[0].lower()
        table_schema_path = 'DB/'+table_name+'_schema.db' 

        if not os.path.exists(table_schema_path):
            print(f"{prompt_msg}No such table") # NoSuchTable
            return
        
        metaDB = db.DB()
        metaDB.open(table_schema_path, dbtype=db.DB_HASH)

        # Get table information
        columns, primary, foreign, not_null = [], [], [], []

        column_names = metaDB.get('column_names'.encode()).decode().split('COLUMN')
        for column_name in column_names:
            columns.append((column_name, metaDB.get(column_name.encode()).decode()))
        primary = metaDB.get('primary_key'.encode()).decode().split('COLUMN')
        foreign_keys = metaDB.get('foreign_key'.encode()).decode().split('FOREIGN')
        for foreign_key in foreign_keys:
            foreign.extend(foreign_key.split('REFERENCE')[0].split('COLUMN'))
        not_null = metaDB.get('not_null'.encode()).decode().split('COLUMN')
        metaDB.close()

        # Print table information
        print("-----------------------------------------------------------------")
        print(f"table_name [{table_name}]")
        headers = ["column_name", "type", "null", "key"]
        print("{:20} {:15} {:10} {:10}".format(*headers))
        for column_name, column_type in columns:
            null, key = 'Y', ''
            if 'char' in column_type:
                column_type = 'char' + '(' + column_type[4:] + ')'
            if column_name in primary:
                key = 'PRI'
                null = 'N'
            elif column_name in not_null:
                null = 'N'
            if column_name in foreign:
                key = 'FOR'
            row = [column_name, column_type, null, key]
            print("{:20} {:15} {:10} {:10}".format(*row))
        print("-----------------------------------------------------------------")


    def show_tables_query(self, items):
        print("-----------------------------------------------------------------")
        files = glob('DB/*_schema.db')
        for file in files:
            name = os.path.splitext(file)[0]
            table_name = name[3:-7]
            print(table_name)
        print("-----------------------------------------------------------------")

 
    def select_query(self, items):
        referred_table_iter = items[2].find_data('referred_table')
        # TODO
        table_names = []
        for referred_table in referred_table_iter:
            table_name = referred_table.children[0].children[0].lower()
            table_names.append(table_name)

        selected_column_iter = items[1].find_data('selected_column')
        selected_columns = []
        for selected_column in selected_column_iter:
            column_name = selected_column.children[1].children[0].lower()
            selected_columns.append(column_name)
        
        #TODO
        if selected_columns:
            pass
        else: # When select all
            table_path = 'DB/'+table_name+'.db'
            table_schema_path = 'DB/'+table_name+'_schema.db'

            if not os.path.exists(table_schema_path):
                print(f"{prompt_msg}Selection has failed: '{table_name}' does not exist") # SelectTableExistenceError
                return
            
            metaDB = db.DB()
            metaDB.open(table_schema_path, dbtype=db.DB_HASH)
            column_names = metaDB.get('column_names'.encode()).decode().split('COLUMN')
            line = '-'  * 20 * len(column_names)
            print(line)
            print(("{:<20} " * len(column_names)).format(*column_names))
            print(line)

            mainDB = db.DB()
            mainDB.open(table_path, dbtype=db.DB_HASH)

            cursor = mainDB.cursor()
            while x := cursor.next():
                key, _ = x
                column_values = key.decode().split('COLUMN')
                print(("{:<20} " * len(column_names)).format(*column_values))
            print(line)


    def insert_query(self, items):
        table_name = items[2].children[0].lower()
        table_path = 'DB/'+table_name+'.db' 
        table_schema_path = 'DB/'+table_name+'_schema.db' 

        if not os.path.exists(table_schema_path):
            print(f"{prompt_msg}No such table") # NoSuchTable
            return
        
        metaDB = db.DB()
        metaDB.open(table_schema_path, dbtype=db.DB_HASH)
        
        column_names = metaDB.get('column_names'.encode()).decode().split('COLUMN')

        # Get inserted column names if insert has
        has_column_names = False
        insert_column_names = []
        if items[3]:
            has_column_names = True
            insert_column_iter = items[3].find_data('column_name')
            for insert_column in insert_column_iter:
                insert_column_names.append(insert_column.children[0])

        # Get inserted types and values
        inserted_types, inserted_values = [], []
        insert_value_iter = items[5].find_data('insert_value')
        for insert_value in insert_value_iter:
            type = insert_value.children[0].type.lower()
            value = insert_value.children[0]
            if value[0] == "'" or value[0] == '"':
                value = value[1:-1]
            inserted_types.append(type)
            inserted_values.append(value)

        # Get primary key and not null constraint
        primary_key = metaDB.get('primary_key'.encode()).decode()
        if primary_key:
            primary_key = primary_key.split('COLUMN')
        else:
            primary_key = []

        not_null = metaDB.get('not_null'.encode()).decode()
        if not_null:
            not_null = not_null.split('COLUMN')
        else:
            not_null = []

        # Compare column numbers
        mismatch = False
        table_columns_len = len(column_names)
        inserted_values_len = len(inserted_values)
        if table_columns_len != inserted_values_len:
            mismatch = True
        if has_column_names:
            insert_columns_len = len(insert_column_names)
            if insert_columns_len != table_columns_len or insert_columns_len != inserted_values_len:
                mismatch = True
        if mismatch:
            print(f"{prompt_msg}Insertion has failed: Types are not matched") # InsertTypeMismatchError
            return

        # Compare column types and existence and check not null constraint
        if has_column_names:
            for i, insert_column_name in enumerate(insert_column_names):

                # Check column existence
                if not metaDB.exists(insert_column_name.encode()):
                    print(f"{prompt_msg}Insertion has failed: '{insert_column_name}' does not exist") # InsertColumnExistenceError
                    return
                
                column_type = metaDB.get(insert_column_name.encode()).decode()
                inserted_type = inserted_types[i]
                
                # Check not null constraint
                if inserted_type == 'null' and (insert_column_name in primary_key or insert_column_name in not_null):
                    print(f"{prompt_msg}Insertion has failed: '{insert_column_name}' is not nullable")
                    return
                
                # Check column types
                if 'char' in column_type:
                    column_type = 'str'
                if inserted_type != 'null' and column_type != inserted_type:
                    print(f"{prompt_msg}Insertion has failed: Types are not matched") # InsertTypeMismatchError
                    return
        else: # When insert has no column names
            for i, table_column_name in enumerate(column_names):
                table_column_type = metaDB.get(table_column_name.encode()).decode()
                inserted_type = inserted_types[i]

                # Check not null constraint
                if inserted_type == 'null' and (table_column_name in primary_key or table_column_name in not_null):
                    print(f"{prompt_msg}Insertion has failed: '{table_column_name}' is not nullable") # InsertColumnNonNullableError
                    return
                
                # Check column types
                if 'char' in table_column_type:
                    table_column_type = 'str'
                if inserted_type != 'null' and table_column_type != inserted_type:
                    print(inserted_type)
                    print(f"{prompt_msg}Insertion has failed: Types are not matched") # InsertTypeMismatchError
                    return

        # When no error occured, insert values
        mainDB = db.DB()
        mainDB.open(table_path, dbtype=db.DB_HASH, flags=db.DB_CREATE)

        if has_column_names:
            for i, (value, insert_column_name) in enumerate(zip(inserted_values, insert_column_names)):
                column_type = metaDB.get(insert_column_name.encode()).decode()
                if 'char' in column_type:
                    char_len = int(column_type[4:])
                    inserted_values[i] = value[:char_len]
            insert_values = []
            for column_name in column_names:
                i = insert_column_names.index(column_name)
                insert_values.append(inserted_values[i])
            mainDB.put('COLUMN'.join(insert_values).encode(), ''.encode())
        else: # When insert has no column names
            for i, (value, column_name) in enumerate(zip(inserted_values, column_names)):
                column_type = metaDB.get(column_name.encode()).decode()
                if 'char' in column_type:
                    char_len = int(column_type[4:])
                    inserted_values[i] = value[:char_len]
            mainDB.put('COLUMN'.join(inserted_values).encode(), ''.encode())

        # Insert success
        print(f"{prompt_msg}The row is inserted")
        metaDB.close()
        mainDB.close()
        return


    def delete_query(self, items):
        table_name = items[2].children[0].lower()
        table_path = 'DB/'+table_name+'.db' 
        table_schema_path = 'DB/'+table_name+'_schema.db' 

        if not os.path.exists(table_schema_path):
            print(f"{prompt_msg}No such table") # NoSuchTable
            return
        
        metaDB = db.DB()
        metaDB.open(table_schema_path)
        mainDB = db.DB()
        mainDB.open(table_path)

        delete_count = 0
        # TODO: 주석 달기
        if items[3]:
            column_types, table_column_names = [], []
            column_names = metaDB.get('column_names'.encode()).decode().split('COLUMN')
            for column_name in column_names:
                column_type = metaDB.get(column_name.encode()).decode()
                column_types.append(column_type)
                table_column_names.append((table_name, column_name))
            cursor = mainDB.cursor()
            while x := cursor.next():
                key, _ = x
                value = key.decode().split('COLUMN')
                try:
                    if evaluate_bool_expr(items[3].children[1], value, column_names, column_types, table_column_names):
                        mainDB.delete(key)
                        delete_count += 1
                except:
                    return
        else: # When where clause not exists
            cursor = mainDB.cursor()
            while x := cursor.next():
                key, _ = x
                mainDB.delete(key)
                delete_count += 1

        # Delete Success        
        print(f"{prompt_msg}{delete_count} row(s) are deleted")
        metaDB.close()
        mainDB.close()
        return

    def update_tables_query(self, items):
        print(f"{prompt_msg}\'UPDATE\' requested")


def evaluate_bool_expr(tree, value, column_names, column_types, table_column_names):
    booleans = []
    for i, child in enumerate(tree.children):
        # Odd children are 'or', so just add even children
        if i % 2 == 0:
            booleans.append(evaluate_bool_term(child, value, column_names, column_types, table_column_names))
    return any(booleans)


def evaluate_bool_term(tree, value, column_names, column_types, table_column_names):
    booleans = []
    for i, child in enumerate(tree.children):
        # Odd children are 'and', so just add even children
        if i % 2 == 0:
            booleans.append(evaluate_bool_factor(child, value, column_names, column_types, table_column_names))
    return all(booleans)


def evaluate_bool_factor(tree, value, column_names, column_types, table_column_names,):
    is_not = True if tree.children[0] else False
    if tree.children[1].children[0].data == 'predicate':
        if tree.children[1].children[0].children[0].data == 'comparison_predicate':
            comp_operand_iter = tree.children[1].children[0].children[0].find_data('comp_operand')
            comp_operand1 = next(comp_operand_iter)
            comp_operand2 = next(comp_operand_iter)

            if comp_operand1.children[0] != None and comp_operand1.children[0].data == 'comparable_value':
                comp_operand1_value = comp_operand1.children[0].children[0]
                comp_operand1_type = comp_operand1.children[0].children[0].type
            else: # When operand is column
                column1 = comp_operand1.children[1].children[0]
                if comp_operand1.children[0]:
                    table1 = comp_operand1.children[0].children[0]
                else:
                    table1 = ''
                if table1:
                    if column1 not in column_names:
                        print(f"{prompt_msg}Where clause trying to reference non existing column") # WhereColumnNotExist
                        raise
                    elif (table1, column1) not in table_column_names:
                        print(f"{prompt_msg}Where clause trying to reference tables which are not specified") # WhereTableNotSpecified
                        raise
                    else:
                        idx = table_column_names.index((table1, column1))
                        comp_operand1_value = value[idx]
                        comp_operand1_type = column_types[idx].lower()
                else:
                    if column1 not in column_names:
                        print(f"{prompt_msg}Where clause trying to reference non existing column") # WhereColumnNotExist
                        raise
                    elif column_names.count(column1) > 1:
                        print(f"{prompt_msg}Where clause contains ambiguous reference") # WhereAmbiguousReference
                        raise
                    else:
                        idx = column_names.index(column1)
                        comp_operand1_value = value[idx]
                        comp_operand1_type = column_types[idx].lower()

            if comp_operand2.children[0] != None and comp_operand2.children[0].data == 'comparable_value':
                comp_operand2_value = comp_operand2.children[0].children[0]
                comp_operand2_type = comp_operand2.children[0].children[0].type.lower()
            else: # When operand is column
                column2 = comp_operand2.children[1].children[0]
                if comp_operand2.children[0]:
                    table2 = comp_operand2.children[0].children[0]
                else:
                    table2 = ''
                if table2:
                    if column2 not in column_names:
                        print(f"{prompt_msg}Where clause trying to reference non existing column") # WhereColumnNotExist
                        raise
                    elif (table2, column2) not in table_column_names:
                        print(f"{prompt_msg}Where clause trying to reference tables which are not specified") # WhereTableNotSpecified
                        raise
                    else:
                        idx = table_column_names.index((table2, column2))
                        comp_operand2_value = value[idx]
                        comp_operand2_type = column_types[idx].lower()
                else:
                    if column2 not in column_names:
                        print(f"{prompt_msg}Where clause trying to reference non existing column") # WhereColumnNotExist
                        raise
                    elif column_names.count(column2) > 1:
                        print(f"{prompt_msg}Where clause contains ambiguous reference") # WhereAmbiguousReference
                        raise
                    else:
                        idx = column_names.index(column2)
                        comp_operand2_value = value[idx]
                        comp_operand2_type = column_types[idx].lower()

            type_error = False
            if comp_operand1_type == comp_operand2_type:
                pass
            elif 'char' in comp_operand1_type and 'char' in comp_operand2_type:
                pass
            elif 'char' in comp_operand1_type and comp_operand2_type == 'str':
                pass
            elif comp_operand1_type == 'str' and 'char' in comp_operand2_type:
                pass
            else:
                type_error = True;
            
            if type_error:
                print(f"{prompt_msg}Where clause trying to compare incomparable values") # WhereIncomparableError
                raise

            comp_op_iter = tree.children[1].children[0].children[0].find_data('comp_op')
            comp_op = next(comp_op_iter).children[0]

            if comp_op == '>':
                result = True if comp_operand1_value > comp_operand2_value else False
            elif comp_op == '<':
                result = True if comp_operand1_value < comp_operand2_value else False
            elif comp_op == '=':
                result = True if comp_operand1_value == comp_operand2_value else False
            elif comp_op == '!=':
                result = True if comp_operand1_value != comp_operand2_value else False
            elif comp_op == '>=':
                result = True if comp_operand1_value >= comp_operand2_value else False
            elif comp_op == '<=':
                result = True if comp_operand1_value <= comp_operand2_value else False

            if is_not:
                return not result
            else:
                return result
            
        else: # When predicate is null_predicate
            column = tree.children[1].children[0].children[0].children[1].children[0]
            if tree.children[1].children[0].children[0].children[0]:
                table = tree.children[1].children[0].children[0].children[0].children[0]
            else:
                table = ''
            if table:
                if column not in column_names:
                    print(f"{prompt_msg}Where clause trying to reference non existing column") # WhereColumnNotExist
                    raise
                elif (table, column) not in table_column_names:
                    print(f"{prompt_msg}Where clause trying to reference tables which are not specified") # WhereTableNotSpecified
                    raise
                else:
                    idx = table_column_names.index((table, column))
                    column_value = value[idx].lower()
            else:
                if column not in column_names:
                    print(f"{prompt_msg}Where clause trying to reference non existing column") # WhereColumnNotExist
                    raise
                elif column_names.count(column) > 1:
                    print(f"{prompt_msg}Where clause contains ambiguous reference") # WhereAmbiguousReference
                    raise
                else:
                    idx = column_names.index(column)
                    column_value = value[idx].lower()

            is_not_null = tree.children[1].children[0].children[0].children[2].children[1]
            if is_not_null:
                return not column_value == 'null'
            else: # is null
                return column_value == 'null'
            
    else: # When parenthesized_boolean_expr
        if is_not:
            return not evaluate_bool_expr(tree.children[1].children[0].children[1])
        else:
            return evaluate_bool_expr(tree.children[1].children[0].children[1])

    

# run.py starts from this location
with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")
before_query = ""
while True:
    try:
        query = before_query + input(prompt_msg) + ' '
        sem_idx = query.rfind(';') # Index of last ';'
        before_query = query[sem_idx + 1:] # Substring after last ';'

        # If has finished query(s)
        if sem_idx != -1:
            queries = query[:sem_idx].split(';')
            for query in queries:
                output = sql_parser.parse(query + ';')
                transformer = MyTransformer()
                transformer.transform(output)

    # When query is exit; or EXIT;
    except SystemExit:
        exit(0)
        
    # When query has syntax error
    except exceptions.UnexpectedInput:
        print(f"{prompt_msg}Syntax error")

    