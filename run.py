import os
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
        not_null = []
        for column_definition in column_definition_iter:
            column_name = column_definition.children[0].children[0].lower()
            column_type = column_definition.children[1].children[0]
            
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
            
            column_is_not_null = column_definition.children[2]
            if column_is_not_null:
                not_null.append(column_name)
        
        # Put 'not_null' as key and not null column names separated by ',' as value into db
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
        
        # Put 'primary_key' as key and primary key column names separated by ',' as value into db
        myDB.put('primary_key'.encode(), 'COLUMN'.join(primary_key).encode())
        
        # Iterate foreign key constraints to find foreign key column names and their references
        referential_constraint_iter = items[3].find_data("referential_constraint")
        foreign_keys = []
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
                referenced_column_is_non_primary = False
                if referenced_table_primary_key is None:
                    referenced_column_is_non_primary = True
                else:
                    referenced_table_primary_key_list = referenced_table_primary_key.decode().split('COLUMN')
                    if referenced_column_name not in referenced_table_primary_key_list:
                        referenced_column_is_non_primary = True
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
            
            #If this foreign key has no error
            foreign_keys.append('COLUMN'.join(foreign_key)+'REFERENCE'+referenced_table_name+'REFERENCE'+'COLUMN'.join(referenced_key))
            referencedDB.close()
            
        # If all foreign keys have no error
        myDB.put('foreign_key'.encode(), 'FOREIGN'.join(foreign_keys).encode())
                
        # Create table success
        print(f"{prompt_msg}\'{table_name}\' table is created")
        myDB.close()

    def drop_table_query(self, items):
        print(f"{prompt_msg}\'DROP TABLE\' requested")
    
    def explain_query(self, items):
        print(f"{prompt_msg}\'EXPLAIN\' requested")
    
    def describe_query(self, items):
        print(f"{prompt_msg}\'DESCRIBE\' requested")

    def desc_query(self, items):
        print(f"{prompt_msg}\'DESC\' requested")
    
    def show_tables_query(self, items):
        print(f"{prompt_msg}\'SHOW TABLES\' requested")

    def select_query(self, items):
        print(f"{prompt_msg}\'SELECT\' requested")

    def insert_query(self, items):
        print(f"{prompt_msg}\'INSERT\' requested")

    def delete_query(self, items):
        print(f"{prompt_msg}\'DELETE\' requested")

    def update_tables_query(self, items):
        print(f"{prompt_msg}\'UPDATE\' requested")


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

    