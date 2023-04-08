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
        
        myDB = db.DB()
        myDB.open(table_schema_path, dbtype=db.DB_HASH, flags=db.DB_CREATE)

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
                myDB.put(column_name.encode(), column_type.encode(), flags=db.DB_NOOVERWRITE)
            except:
                print(f"{prompt_msg}Create table has failed: column definition is duplicated") # DuplicateColumnDefError
                os.remove(table_schema_path)
                return
            
            column_names.append(column_name)

            column_is_not_null = column_definition.children[2]
            if column_is_not_null:
                not_null.append(column_name)

        # Put 'column_names' as key and column names separated by 'COLUMN' as value into db
        myDB.put('column_names'.encode(), 'COLUMN'.join(column_names).encode())
        
        # Put 'not_null' as key and not null column names separated by 'COLUMN' as value into db
        myDB.put('not_null'.encode(), 'COLUMN'.join(not_null).encode())

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
                if not myDB.exists(primary_column_name.encode()):
                    print(f"{prompt_msg}Create table has failed: {primary_column_name} does not exist in column definition") # NonExistingColumnDefError
                    os.remove(table_schema_path)
                    return
                primary_key.append(primary_column_name)
            is_duplicate_primary_key = True
        
        # Put 'primary_key' as key and primary key column names separated by 'COLUMN' as value into db
        myDB.put('primary_key'.encode(), 'COLUMN'.join(primary_key).encode())

        # Put 'reference_count' as key and 0 as value
        myDB.put('reference_count'.encode(), '0'.encode())
        
        # Iterate foreign key constraints to find foreign key column names and their references
        referential_constraint_iter = items[3].find_data("referential_constraint")
        foreign_keys, referenced_tables = [], []
        for referential_constraint in referential_constraint_iter:
            foreign_key, referenced_key = [], []

            referencing_column_iter = referential_constraint.children[2].find_data("column_name")
            for referencing_column in referencing_column_iter:
                referencing_column_name = referencing_column.children[0].lower()
                if not myDB.exists(referencing_column_name.encode()):
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
            referenced_table_primary_key_list = referenced_table_primary_key.decode().split('COLUMN')
            for referencing_column_name, referenced_column_name in zip(foreign_key, referenced_key):
                if not referencedDB.exists(referenced_column_name.encode()):
                    print(f"{prompt_msg}Create table has failed: foreign key references non existing column") # ReferenceColumnExistenceError
                    os.remove(table_schema_path)
                    referencedDB.close()
                    return
                
                referenced_table_primary_key = referencedDB.get('primary_key'.encode())
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
                
                referencing_column_type = myDB.get(referencing_column_name.encode()).decode()
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
        myDB.put('foreign_key'.encode(), 'FOREIGN'.join(foreign_keys).encode())
        if foreign_keys:
            for referenced_table_schema_path in referenced_tables:
                referencedDB = db.DB()
                referencedDB.open(referenced_table_schema_path, dbtype=db.DB_HASH)
                reference_count = str(int(referencedDB.get('reference_count'.encode()).decode()) + 1)
                referencedDB.put('reference_count'.encode(), reference_count.encode())
                referencedDB.close()
                
        # Create table success
        print(f"{prompt_msg}\'{table_name}\' table is created")
        myDB.close()


    def drop_table_query(self, items):
        table_name = items[2].children[0].lower()
        table_schema_path = 'DB/'+table_name+'_schema.db' 

        if not os.path.exists(table_schema_path):
            print(f"{prompt_msg}No such table") # NoSuchTable
            return
        
        myDB = db.DB()
        myDB.open(table_schema_path, dbtype=db.DB_HASH)

        if myDB.get('reference_count'.encode()).decode() != '0':
            print(f"{prompt_msg}Drop table has failed: '{table_name}' is referenced by other table") # DropReferencedTableError
            myDB.close()
            return
        
        # If drop has no errors, delete metadata of referenced by this table in referenced table
        if myDB.get('foreign_key'.encode()).decode():
            foreign_keys = myDB.get('foreign_key'.encode()).decode().split('FOREIGN')
            for foreign_key in foreign_keys:
                referenced_table_name = foreign_key.split('REFERENCE')[1]
                referenced_table_schema_path = 'DB/'+referenced_table_name+'_schema.db'
                referencedDB = db.DB()
                referencedDB.open(referenced_table_schema_path, dbtype=db.DB_HASH)
                reference_count = str(int(referencedDB.get('reference_count'.encode()).decode()) - 1)
                referencedDB.put('reference_count'.encode(), reference_count.encode())
                
        # Drop success
        os.remove(table_schema_path)
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
        
        myDB = db.DB()
        myDB.open(table_schema_path, dbtype=db.DB_HASH)

        # Get table information
        columns, primary, foreign, not_null = [], [], [], []

        column_names = myDB.get('column_names'.encode()).decode().split('COLUMN')
        for column_name in column_names:
            columns.append((column_name, myDB.get(column_name.encode()).decode()))
        primary = myDB.get('primary_key'.encode()).decode().split('COLUMN')
        foreign_keys = myDB.get('foreign_key'.encode()).decode().split('FOREIGN')
        for foreign_key in foreign_keys:
            foreign.extend(foreign_key.split('REFERENCE')[0].split('COLUMN'))
        not_null = myDB.get('not_null'.encode()).decode().split('COLUMN')
        myDB.close()

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
            table_schema_path = 'DB/'+table_name+'_schema.db'

            if not os.path.exists(table_schema_path):
                print(f"{prompt_msg}Selection has failed: '{table_name}' does not exist") # SelectTableExistenceError
                return
            
            myDB = db.DB()
            myDB.open(table_schema_path, dbtype=db.DB_HASH)
            column_names = myDB.get('column_names'.encode()).decode().split('COLUMN')
            line = '-'  * 20 * len(column_names)
            print(line)
            print(("{:<20} " * len(column_names)).format(*column_names))
            print(line)

            mainDB = db.DB()
            mainDB.open(table_path, dbtype=db.DB_HASH)

            if has_primary_key(myDB):
                cursor = mainDB.cursor()
                while x := cursor.next():
                    _, value = x
                    column_values = value.decode().split('COLUMN')
                    print(("{:<20} " * len(column_names)).format(*column_values))
                print(line)
            else:
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
        
        myDB = db.DB()
        myDB.open(table_schema_path, dbtype=db.DB_HASH)
        
        column_names = myDB.get('column_names'.encode()).decode().split('COLUMN')

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

        primary_key = myDB.get('primary_key'.encode()).decode()
        if primary_key:
            #TODO in project 1-3
            primary_key = primary_key.split('COLUMN')
            return
        else: # When table has no primary key
            mainDB = db.DB()
            mainDB.open(table_path, dbtype=db.DB_HASH, flags=db.DB_CREATE)

            for i, (type, value, column_name) in enumerate(zip(inserted_types, inserted_values, column_names)):
                #TODO in project 1-3
                column_type = myDB.get(column_name.encode()).decode()
                if 'char' in column_type:
                    char_len = int(column_type[4:])
                    inserted_values[i] = value[:char_len]
            mainDB.put('COLUMN'.join(inserted_values).encode(), ''.encode())

        # Insert success
        myDB.close()
        mainDB.close()
        print(f"{prompt_msg}The row is inserted")


    def delete_query(self, items):
        print(f"{prompt_msg}\'DELETE\' requested")


    def update_tables_query(self, items):
        print(f"{prompt_msg}\'UPDATE\' requested")


def has_primary_key(DB):
    if DB.get('primary_key'.encode()):
        return True
    else:
        return False


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

    