import os
import os.path
import re
import sys
import zipfile
import argparse
import logging
import sys

import psycopg
import dotenv

import config
import dbutils

logger = logging.getLogger(__name__)

def add_schema(ddl_file:str,schema_name:str,vocab_schema_name:str)->str:
    '''
    Reads an OMOP DDL file as downloaded from the OHDSI github and replaces the schema template variable
     with the specified schema.

    :param ddl_file: The path to the OMOP DDL file.
    :type ddl_file: str
    :param schema_name: The schema name to replace the @cdmDatabaseSchema placeholder with.
    :type schema_name: str
    :param vocab_schema_name: The schema name to replace the @cdmDatabaseSchema placeholder with for vocab tablees
    :type vocab_schema_name: str

    :returns: A string containing the contents of the ddl file with the specifed schema set.
    :rtype: str
    '''
    logger.debug("Adding Schema %s to DDL file %s" % (schema_name,ddl_file))
    with open(ddl_file) as f:
        ddl = f.read()
        table_creates = re.findall('CREATE TABLE ([^\s]+)',ddl)
        for create_statement in table_creates:
            table_name = create_statement.split('.')[1]
            if dbutils.is_vocab_table(table_name):
                 ddl = ddl.replace(create_statement,'%s.%s' % (vocab_schema_name,table_name))
            else:
                ddl = ddl.replace(create_statement,'%s.%s' % (schema_name,table_name))
        ddl = ddl.replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ')
    return ddl

def run_sql_template(conn:psycopg.connection,schema_name:str,vocab_schema_name:str,template_file:str)->None:
    """
    Executes a file of SQL statements as downloaded from the OHDSI github. The contents
    of the file are first passed to the add_schema function to replace the schema placeholder.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The schema name to run the statements against.
    :type schema_name: str
    :param vocab_schema_name: The schema name to run the statements against for vocab tables.
    :type vocab_schema_name: str
    :param template_file: The file of sql statements to run.
    :type temple_file: str

    :returns: None
    :rtype: None
    """
    logger.debug("Running sql file %s on database %s schema %s" % (template_file,conn,schema_name))
    sql_queries = add_schema(template_file,schema_name,vocab_schema_name)
    with conn.cursor() as cur:
        cur.execute(sql_queries)
    return None

def drop_cdm(conn:psycopg.connection,schema_name:str,vocab_schema_name:str,results_schema_name:str)->None:
    """
    Drops the specifed schemas from the database. Does nothing if they calready exist.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema to remove.
    :type schema_name: str
    :param vocab_schema_name: The name of the vocab schema to remove.
    :type vocab_schema_name: str
    :param results_schema_name: The name of the results schema to remove.
    :type results_schema_name: str

    :returns: None
    :rtype: None
    """
    if dbutils.schema_exists(conn,schema_name):
        logger.debug("Dropping schema %s" % schema_name)
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA %s CASCADE' % (schema_name,))
        conn.commit()
    else:
        logger.debug("Schema %s does not exist. Not dropping" % (schema_name,))
    if dbutils.schema_exists(conn,vocab_schema_name):
        logger.debug("Dropping schema %s" % (vocab_schema_name,))
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA %s CASCADE' % (vocab_schema_name,))
        conn.commit()
    else:
        logger.debug("Schema %s does not exist. Not dropping" % (vocab_schema_name,))
    if dbutils.schema_exists(conn,results_schema_name):
        logger.debug("Dropping schema %s" % (results_schema_name,))
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA %s CASCADE' % (results_schema_name,))
        conn.commit()
    else:
        logger.debug("Schema %s does not exist. Not dropping" % (results_schema_name,))
    return None

def build_cdm(conn:psycopg.connection,schema_name:str,vocab_schema_name:str,ddl_file:str,results_schema_name:str)->None:
    """
    Build the OMOP CDM Tables by executing the OMOP DDL file. 
    Does nothing if the already exist (by replacing the CREATE TABLE statements with CREATE TABLE IF NOT EXISTS statements)

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema to create and build the tables in. This replaces the schema template variable in the DDL file.
    :type schema_name: str
    :param vocab_schema_name: The name of the CDM schema to create and build the tables in. This replaces the schema template variable in the DDL file for vocab tables.
    :type vocab_schema_name: str
    :param results_schema_name: The name of the results schema to create and build the results the tables in (not currently used)
    :type results_schema_name: str

    :returns: None
    :rtype: None
    """
    if not dbutils.schema_exists(conn,schema_name):
        logger.debug("Creating schema %s" % schema_name)
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA %s' % (schema_name,))
        conn.commit()
    else:
        logger.debug("Schema %s exists. Not creating" % schema_name)
    if not dbutils.schema_exists(conn,vocab_schema_name):
        logger.debug("Creating schema %s" % vocab_schema_name)
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA %s' % (vocab_schema_name,))
        conn.commit()
    else:
        logger.debug("Schema %s exists. Not creating" % schema_name)
    if not dbutils.schema_exists(conn,results_schema_name):
        logger.debug("Creating schema %s" % results_schema_name)
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA %s' % (results_schema_name,))
        conn.commit()
    else:
        logger.debug("Schema %s exists. Not creating" % results_schema_name)
    #TODO Change this to go table by table getting the correct schema as we go.
    run_sql_template(conn,schema_name,vocab_schema_name,ddl_file)
    return None

def build_indicies(conn:psycopg.connection,schema_name:str,vocab_schema_name:str,indices_file:str)->None:
    """
    Build the OMOP CDM Indexes by executing the OMOP Indexes file. Does nothing if they already exist.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file.
    :type schema_name: str
    :param vocab_schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file for vocab tables.
    :type vocab_schema_name: str
    :param indices_file: The name of the file containing the SQL statements to create the indexes.
    :type indices_file: str

    :returns: None
    :rtype: None
    """
    cluster_commands = []
    created_indexes = []
    with open(indices_file) as f:
            for line in f:
                sql = None
                index_name = re.match('CREATE INDEX (.+) ON ([^\s]+)',line)
                if index_name is None:
                    continue
                table_name = index_name.group(2)
                index_name = index_name.group(1)
                logger.debug("Got index_name %s" % index_name)
                if not index_name is None:
                    sql = line
                    with conn.cursor() as cur:
                        if not dbutils.index_exists(conn,index_name):
                            if dbutils.is_vocab_table(table_name):
                                sql = sql.replace('@cdmDatabaseSchema',vocab_schema_name)
                            else:
                                sql = sql.replace('@cdmDatabaseSchema',schema_name)
                            index_name = sql.split()[2]
                            cur.execute(sql)
                            created_index = index_name
                            created_indexes.append(created_index)
                            logger.debug("Created index %s" % created_index)
                        else:
                            logger.debug("Skipped index %s" % index_name)                    
                elif line.startswith('CLUSTER'):
                    cluster_commands.append(line)
    with conn.cursor() as cur:
        for cluster_command in cluster_commands:
            index_name = cluster_command.split()[3]
            sql = cluster_command.replace('@cdmDatabaseSchema',schema_name).strip()
            if index_name in created_indexes:
                logger.debug("Running %s" % sql)
                cur.execute(sql)
            else:
                logger.debug("Skipping %s" % sql)
    return None

def build_pkeys(conn:psycopg.connection,schema_name:str,vocab_schema_name:str,pkeys_file:str)->None:
    """
    Build the OMOP CDM Primary Keys by executing the OMOP Primary Keys file. Does nothing if they already exist.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file.
    :type schema_name: str
    :param vocab_schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file for vocab tables.
    :type vocab_schema_name: str
    :param pkeys_file: The name of the file containing the SQL statements to create the Keys.
    :type pkeys_file: str

    :returns: None
    :rtype: None
    """
    with open(pkeys_file) as f:
        for line in f:
            with conn.cursor() as cur:
                key_name = re.search('ALTER TABLE (.+) ADD CONSTRAINT (.+) PRIMARY KEY',line)
                if key_name is None:
                    continue
                table_name = key_name.group(1)
                key_name = key_name.group(2)
                logger.debug("Got key name %s" % key_name)
                logger.debug("Got table name %s" % table_name)
                if dbutils.is_vocab_table(table_name):
                    logger.debug("Making key in vocab schema %s" % vocab_schema_name)
                    sql = line.replace('@cdmDatabaseSchema',vocab_schema_name).strip()
                else:
                    logger.debug("Making key in cdm schema %s" % schema_name)
                    sql = line.replace('@cdmDatabaseSchema',schema_name).strip()
                if not dbutils.key_exists(conn,key_name):
                    cur.execute(sql)
                    logger.debug("Added key %s" % sql)
                else:
                    logger.debug("Skipped key %s" % sql)
    return None

def build_fkeys(conn:psycopg.connection,schema_name:str,vocab_schema_name:str,constraints_file:str)->None:
    """
    Build the OMOP CDM foreign keys by executing the OMOP Constrains file. Does nothing if they already exist.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file.
    :type schema_name: str
    :param vocab_schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file for vocab tables.
    :type vocab_schema_name: str
    :param constraints_file: The name of the file containing the SQL statements to create the foreign keys.
    :type constraints_file: str

    :returns: None
    :rtype: None
    """
    with open(constraints_file) as f:
        for line in f:
            with conn.cursor() as cur:
                key_name = re.search('ALTER TABLE (.+) ADD CONSTRAINT (.+) FOREIGN KEY .+ REFERENCES ([^\s]+)',line)
                if key_name is None:
                    continue
                logger.debug("Got SQL line %s" % line)
                table_name = key_name.group(1)
                reference_table_name = key_name.group(3)
                key_name = key_name.group(2)
                logger.debug("Got foreign key name %s" % key_name)
                logger.debug("Got table name %s" % table_name)
                logger.debug("Got reference table name %s" % reference_table_name)
                if dbutils.is_vocab_table(table_name):
                    logger.debug("Making foreign key in vocab schema %s" % vocab_schema_name)
                    sql = line.replace(table_name,"%s.%s" % (vocab_schema_name,table_name.split('.')[1])).strip()
                else:
                    logger.debug("Making foreign key in cdm schema %s" % schema_name)
                    sql = line.replace(table_name,"%s.%s" % (schema_name,table_name.split('.')[1])).strip()
                if dbutils.is_vocab_table(reference_table_name):
                    logger.debug("Referencing table in vocab schema %s" % reference_table_name)
                    sql = sql.replace(reference_table_name,"%s.%s" % (vocab_schema_name,reference_table_name.split('.')[1])).strip()
                else:
                    logger.debug("Referencing table in cdm schema %s" % reference_table_name)
                    sql = sql.replace(reference_table_name,"%s.%s" % (schema_name,reference_table_name.split('.')[1])).strip()
                if not dbutils.key_exists(conn,key_name):
                    cur.execute(sql)
                    logger.debug("Added foreign key %s" % sql)
                else:
                    logger.debug("Skipped foreign key %s" % sql)
    return None

def build_table_map(data_pattern:str,data_path:str)->list[tuple[str,str]]: 
    """
    Reads the files in a folder and uses a regular expression to extract the OMOP table name from the file name.

    :param conn: A psycopg connection object to the postgres database
    :type data_pattern: str
    :param data_path: The folder containing data files to be loaded into OMOP tables.
    :type data_path: str

    :returns: A a list of tuples of (file name,omop table name)
    :rtype: tuple
    """
    data_pattern = re.compile(data_pattern)
    table_map = []
    file_names = os.listdir(data_path)
    for name in file_names:
        name_match = data_pattern.match(name)
        if name_match:
            tmap = [os.path.join(data_path,name),name_match.groupdict()['tablename']]
            logger.debug("Made file to table map %s" % (tmap,))
            table_map.append(tmap)
    return table_map

def load_vocabs_from_zip(conn:psycopg.connection,db_schema:str,zip_file:str)->None:
    """
    Loads OMOP vocabluaries from a zip file as downloaded from Athena. 
    N.B. This does not currently handle vocabs which require a license/post processing (e.g. CPT4).

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema. This replaces the schema template variable in the sql file for the vocab tables.
    :type schema_name: str
    :param zip_file: The path to the zip file containing vocab files.
    :type zip_file: str

    :returns: None
    :rtype: None
    """
    vocab_files = ['CONCEPT.csv','CONCEPT_ANCESTOR.csv','CONCEPT_CLASS.csv','CONCEPT_RELATIONSHIP.csv','CONCEPT_SYNONYM.csv','DOMAIN.csv',
                   'DRUG_STRENGTH.csv','RELATIONSHIP.csv','VOCABULARY.csv']
    logger.debug("Loading vocabs from %s" % zip_file)
    archive = zipfile.ZipFile(zip_file, 'r')
    for vocab_file in vocab_files:
        with conn.cursor() as cur:
            with archive.open(vocab_file) as f:
                logger.debug("Checking %s" % vocab_file)
                table_name = vocab_file.replace(".csv","")
                if dbutils.table_is_empty(conn,db_schema,table_name):
                    logger.debug("Loading %s" % table_name)
                    query = "COPY %s.%s FROM STDIN WITH(FORMAT CSV, HEADER, DELIMITER E'\\t', QUOTE E'\\b')" % (db_schema,table_name,)
                    logger.debug(query)
                    with cur.copy(query) as copy:
                        while data := f.read(100):
                            copy.write(data)
                else:
                    logger.debug("Skippng table %s" % table_name)
    return None

def load_data_csv(conn:psycopg.connection,db_schema:str,table_map:tuple[str,str],delete_first=False)->None:
    """
    Loads data from CSV files into OMOP tables. Expects one file per table. 
    N.B. This will not load data into any table which already contains data (i.e if count(*)>0).

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file.
    :type schema_name: str
    :param table_map: A list of tuples specifying a fully qualified file path and an OMOP table name to load the data into i.e [(file name,table name)].
    :type table_map: list(tuple)
    :param delete_first: Delete all rows from table before loading data. Defaults to False. Data will not be loaded to any table contaning data.
    :type delete_first: bool

    :returns: None
    :rtype: None
    """
    disabled_table_list = []
    for csv_file,table_name in table_map:
        logger.debug("Got file %s for table %s" % (csv_file,table_name))
        with open(csv_file) as f:
                headers = f.readline().strip()
        logger.debug("Got CSV headers:%s" % headers)
        if dbutils.table_is_empty(conn,db_schema,table_name) or delete_first:                
            logger.debug("Loading table %s" % table_name)
            with conn.cursor() as cur:
                if delete_first:
                    logger.debug("Delete contents of %s" % table_name)
                    cur.execute("ALTER TABLE %s.%s DISABLE TRIGGER ALL" % (db_schema,table_name))
                    disabled_table_list.append("%s.%s" % (db_schema,table_name))
                    cur.execute("DELETE FROM %s.%s" % (db_schema,table_name))
                with open(csv_file) as f:
                    query = 'COPY %s.%s (%s) FROM STDIN WITH(FORMAT CSV, HEADER)' % (db_schema,table_name,headers)
                    with cur.copy(query) as copy:
                        while data := f.read(100):
                            copy.write(data)
                for table in disabled_table_list:
                    cur.execute("ALTER TABLE %s.%s ENABLE TRIGGER ALL" % (db_schema,table_name))
        else:
            logger.debug("Table %s not empty. Skipping" % (table_name,))
    return None

def get_args_parser()->argparse.ArgumentParser:
    """
    Builds a parser to handle the command line arguments.

    :returns: Parser for arguments
    :rtype: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--debug", 
                        help='Display debug logging.',
                        action='store_true'
                        )
    parser.add_argument("-dr","--dryrun", 
                        help='Rollback the transaction on completion.',
                        action='store_true'
                        )
    parser.add_argument("-sc","--skipcheck", 
                        help='Skips checking the state of the database before running action.',
                        action='store_true'
                        )
    parser.add_argument("--omopschema", 
                        help='OMOP Schema. Overrides config.DB_OMOP_SCHEMA',
                        )
    parser.add_argument("--vocabschema", 
                        help='Vocab Schema. Overrides config.DB_VOCAB_SCHEMA',
                        )

    subparsers = parser.add_subparsers(help='Database operation',
                                       dest='action')
    subparsers.required = True
    #parser.add_argument("action", 
    #                    help='The build action to complete.',
    #                    type=str,
    #                    choices = ["clean","build","vocabs","load","pkeys","index","fkeys","all","reload"],
    #                    )
    parser_op_clean = subparsers.add_parser('clean', help='Removes all objects by deleting the schemas')
    parser_op_build = subparsers.add_parser('build', help='Builds the CDM Tables')
    parser_op_vocabs = subparsers.add_parser('vocabs', help='Loads the Vocabularies')
    parser_op_load = subparsers.add_parser('load', help='Loads the CSV data')
    parser_op_pkeys = subparsers.add_parser('pkeys', help='Builds the primary keys')
    parser_op_index = subparsers.add_parser('index', help='Builds the indexes')
    parser_op_fkeys = subparsers.add_parser('fkeys', help='Builds the foreign keys')
    parser_op_all = subparsers.add_parser('all', help='Runs all actions except for clean')
    parser_op_reload = subparsers.add_parser('reload', help='Reloads the CSV data')

    return parser

def handle_args()->argparse.Namespace:
    """
    Parses the command line arguments.

    :returns: Namespace containing parsed arguments
    :rtype: argparse.Namespace
    """
    parser = get_args_parser()
    args = parser.parse_args()
    if not args.omopschema is None:
        config.DB_OMOP_SCHEMA = args.omopschema
    if not args.vocabschema is None:
        config.DB_VOCAB_SCHEMA = args.vocabschema
    return args

def setup_logging(debug:bool)->None:
    """
    Sets the logging level. Sets to DEBUG if debug paramter is true, otherwise sets INFO

    :param debug: Specifes whether debug should be set.
    :type debug: boolean

    :returns: None
    :rtype: None
    """
    ch = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.info("Log level DEBUG")
    else:
        logger.setLevel(logging.INFO)
        logger.info("Log level INFO")
    return None
    
def clean(conn:psycopg.connection)->None: #action=="clean"
    """
    Calls :py:func:`drop_cdm` with the values of  :py:data:`config.DB_OMOP_SCHEMA`, :py:data:`config.DB_VOCAB_SCHEMA`
    and :py:data:`config.DB_RESULTS_SCHEMA`

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    
    :returns: None
    :rtype: None
    """
    logger.info("Dropping schemas",)
    drop_cdm(conn,config.DB_OMOP_SCHEMA,config.DB_VOCAB_SCHEMA,config.DB_RESULTS_SCHEMA) 
    return None

def build(conn:psycopg.connection)->None: #action=="cdm"
    """
    Calls :py:func:`build_cdm` with the values of  :py:data:`config.DB_OMOP_SCHEMA`,
    :py:data:`config.DDL_FILE` and  :py:data:`config.DB_RESULTS_SCHEMA`

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection

    :returns: None
    :rtype: None
    """
    logger.info("Building cdm")
    build_cdm(conn,config.DB_OMOP_SCHEMA,config.DB_VOCAB_SCHEMA,config.DDL_FILE,config.DB_RESULTS_SCHEMA)
    return None

def vocabs(conn:psycopg.connection,skip_check:bool=False)->None: #action=="vocabs":
    """
    Ensures tables are built by calling :py:func:`build()` and then Calls :py:func:`load_vocabs_file_zip` 
    with the values of :py:data:`config.DB_OMOP_SCHEMA`, :py:data:`config.VOCABS_ZIP`.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param skip_check: If true, no check is performed on the state of the database first.
    :type conn: bool

    :returns: None
    :rtype: None
    """
    if not skip_check:
        build(conn) 
    logger.info("Loading vocabs")
    load_vocabs_from_zip(conn,config.DB_VOCAB_SCHEMA,config.VOCABS_ZIP) #action=="vocabs" #TODO Clean vocabs?
    return None

def load(conn:psycopg.connection,delete_first:bool=False,skip_check:bool=False)->None: #action=="load"
    """
    Ensures vocabs are loaded by calling :py:func:`vocabs()`, builds a table to file map and then calls 
    :py:func:`load_data_csv` with the values of and :py:data:`config.DB_OMOP_SCHEMA`.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param delete_first: Delete data from each table before loading
    :type delete_first: bool
    :param skip_check: If true, no check is performed on the state of the database first.
    :type conn: bool

    :returns: None
    :rtype: None
    """
    if not skip_check:
        vocabs(conn)
    table_map = build_table_map(config.DATA_PATTERN,config.DATA_PATH)
    if delete_first:
        logger.info("Reloading data")
    else:
        logger.info("Loading data")
    load_data_csv(conn,config.DB_OMOP_SCHEMA,table_map,delete_first) 
    return None

def pkeys(conn:psycopg.connection,delete_first=False,skip_check:bool=False)->None:
    """
    Ensures data is loaded by calling :py:func:`load()` then calls :py:func:`build_keys` with the values 
    :py:data:`config.DB_OMOP_SCHEMA`, :py:data:`config.DB_VOCAB_SCHEMA`, and :py:data:`config.KEYS_FILE`.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param delete_first: Delete key from each table and then re-create
    :type delete_first: bool
    :param skip_check: If true, no check is performed on the state of the database first.
    :type conn: bool

    :returns: None
    :rtype: None
    """
    if not skip_check:
        load(conn)
    logger.info("Adding primary keys")
    build_pkeys(conn,config.DB_OMOP_SCHEMA,config.DB_VOCAB_SCHEMA,config.KEYS_FILE)
    return None

def index(conn:psycopg.connection,delete_first=False,skip_check:bool=False)->None:
    """
    Ensures keys are created by calling :py:func:`keys()` then calls :py:func:`build_indicies` with the values 
    :py:data:`config.DB_OMOP_SCHEMA`, :py:data:`config.DB_VOCAB_SCHEMA`, and :py:data:`config.INDICIES_FILE`.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param delete_first: Delete index and then re-create
    :type delete_first: bool
    :param skip_check: If true, no check is performed on the state of the database first.
    :type conn: bool

    :returns: None
    :rtype: None
    """
    if not skip_check:
        pkeys(conn)
    logger.info("Building indexes")
    build_indicies(conn,config.DB_OMOP_SCHEMA,config.DB_VOCAB_SCHEMA,config.INDICIES_FILE)
    return None

def fkeys(conn:psycopg.connection,delete_first=False,skip_check:bool=False)->None:
    """
    Ensures indexes are created by calling :py:func:`indicies()` then calls :py:func:`build_fkeys` with the values 
    :py:data:`config.DB_OMOP_SCHEMA`, :py:data:`config.DB_VOCAB_SCHEMA`, :py:data:`config.CONSTRAINTS_FILE`.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param delete_first: Delete contraints and then re-create
    :type delete_first: bool
    :param skip_check: If true, no check is performed on the state of the database first.
    :type conn: bool

    :returns: None
    :rtype: None
    """
    if not skip_check:
        index(conn)
    logger.info("Adding foreign keys")
    build_fkeys(conn,config.DB_OMOP_SCHEMA,config.DB_VOCAB_SCHEMA,config.CONSTRAINTS_FILE)
    return None

if __name__=="__main__":
    args = handle_args()
    setup_logging(args.debug)
    logger.debug("Running with args: %s" % (args,))
    skip_check = args.skipcheck
    with psycopg.connect(config.DB_CONN_STR) as conn:
        if args.action=='clean':
            clean(conn)
        if args.action=='build' or args.action=='all':
            build(conn)
        if args.action=='vocabs' or args.action=='all':
            vocabs(conn,skip_check)
        if args.action=='load' or args.action=='reload' or args.action=='all':
            reload = (args.action=='reload')
            load(conn,reload,skip_check)
            #TODO We should probably have an option to rebuild indexes etc on a reload?
        if args.action=='pkeys' or args.action=='all':
            pkeys(conn,False,skip_check)
        if args.action=='index' or args.action=='all':
            index(conn,False,skip_check)
        if args.action=='fkeys' or args.action=='all':
            fkeys(conn,False,skip_check)
        if args.dryrun:
            conn.rollback()
        else:
            conn.commit()
