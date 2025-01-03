import psycopg
import logging

logger = logging.getLogger(__name__)

def schema_exists(conn:psycopg.connection,schema_name:str)->bool:
    """
    Checks whether the given schema exists.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the schema to check.
    :type schema_name: str

    :returns: True if the schema exists
    :rtype: bool
    """
    logger.debug("Checking for schema %s" % (schema_name,))
    sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name='%s'" % (schema_name)
    with conn.cursor() as cur:
        res = cur.execute(sql)
        exists = cur.rowcount>0
        logger.debug("Schema exists is %s" % exists)
        return exists
    return None

def key_exists(conn:psycopg.connection,key_name:str)->bool:
    """
    Checks whether the given primary or foreign key exists.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param key_name: The name of the key to check.
    :type key_name: str

    :returns: True if the key exists
    :rtype: bool
    """
    key_name = key_name.strip()
    logger.debug("Checking for key %s" % (key_name,))
    sql = "select constraint_name from information_schema.table_constraints where constraint_name='%s' and (constraint_type='PRIMARY KEY' or constraint_type='FOREIGN KEY')" % key_name
    with conn.cursor() as cur:
        res = cur.execute(sql)
        exists = cur.rowcount>0
        logger.debug("Key exists is %s" % exists)
        return exists
    return None

def index_exists(conn:psycopg.connection,index_name:str)->bool:
    """
    Checks whether the given index exists.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param index_name: The name of the index to check.
    :type index_name: str

    :returns: True if the index exists
    :rtype: bool
    """
    index_name = index_name.strip()
    logger.debug("Checking for Index %s" % (index_name,))
    sql = "select indexname from pg_indexes where indexname='%s'" % index_name
    with conn.cursor() as cur:
        res = cur.execute(sql)
        exists = cur.rowcount>0
        logger.debug("Index exists is %s" % exists)
        return exists
    return None

def table_is_empty(conn:psycopg.connection,schema_name:str,table_name:str)->bool:
    """
    Checks whether the given table is empty.

    :param conn: A psycopg connection object to the postgres database
    :type conn: psycopg.connection
    :param schema_name: The name of the CDM schema. This replaces the schema template varaible in the sql file.
    :type schema_name: str
    :param table_name: The name of the table to check.
    :type table_name: str

    :returns: True if the table is empty
    :rtype: bool
    """
    logger.debug("Checking if table %s is empty" % table_name)
    sql = "SELECT count(*) FROM %s.%s" % (schema_name,table_name)
    with conn.cursor() as cur:
        res = cur.execute(sql)
        count = res.fetchone()[0]
        empty = (count==0)
        logger.debug("empty is %s" % empty)
        return empty
    return None

def is_vocab_table(table_name:str)->bool:
    """
    Checks whether the given table is a vocabulary table.

    :param table_name: The name of the table to check.
    :type table_name: str

    :returns: True if the table is a vocabulary table
    :rtype: bool
    """
    table_name = table_name.split(".") # Split the schema and table name
    table_name = table_name[-1] # Get the table name (even if there was no schema)
    table_name = table_name.lower().strip()
    logger.debug("Checking if %s is a vocab table" % table_name)
    vocab_tables = ['concept', 'concept_ancestor', 'concept_class', 'concept_relationship', 'concept_synonym', 'domain',
                    'drug_strength', 'relationship', 'vocabulary']

    return (table_name in vocab_tables)