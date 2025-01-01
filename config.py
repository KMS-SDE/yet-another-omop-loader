import os

import dotenv

dotenv.load_dotenv()

#: Path to the Postgres DDL file as downloaded from the OHDSI Github. Set from the YAOL_DDL_FILE env var.
DDL_FILE = os.environ.get('YAOL_DDL_FILE')
#: Path to the Postgres constraints sql file as downloaded from the OHDSI Github. Set from the YAOL_CONSTRAINTS_FILE env var.
CONSTRAINTS_FILE = os.environ.get('YAOL_CONSTRAINTS_FILE')
#: Path to the Postgres indexes sql file as downloaded from the OHDSI Github. Set from the YAOL_INDICIES_FILE env var.
INDICIES_FILE = os.environ.get('YAOL_INDICIES_FILE')
#: Path to the Postgres primary keys sql file as downloaded from the OHDSI Github. Set from the YAOL_KEYS_FILE env var.
KEYS_FILE = os.environ.get('YAOL_KEYS_FILE')
#: Path to the CSV files containing omop data. One per table. Set from the YAOL_DATA_PATH env var.
DATA_PATH = os.environ.get('YAOL_DATA_PATH')
#: Regular expression to extract the OMOP table name from the CSV file name. Set from the YAOL_DATA_PATTERN env var.
DATA_PATTERN = os.environ.get('YAOL_DATA_PATTERN','^(?P<tablename>[a-z_]+).csv$')
#: Postgres connection string. Set from the YAOL_DB_CONN_STR env var.
DB_CONN_STR = os.environ.get('YAOL_DB_CONN_STR','postgresql://omop:omop@localhost:5432/omop54')
#DB_CONN_STR = os.environ.get('YAOL_DB_CONN_STR','postgresql://omop@/omop54')
#: Name of the Schema to use for the CDM data tables.  Set from the YAOL_DB_OMOP_SCHEMA env var.
DB_OMOP_SCHEMA = os.environ.get('YAOL_DB_OMOP_SCHEMA','cdm')
#: Name of the Schema to use for the Achilles results tables. Set from the YAOL_DB_RESULTS_SCHEMA env var.
DB_RESULTS_SCHEMA = os.environ.get('YAOL_DB_RESULTS_SCHEMA','results')
#: Path to a zip file containg OMOP vocabulary files as downlaoded from Athena. Set from the YAOL_VOCAB_ZIP env var.
VOCABS_ZIP = os.environ.get('YAOL_VOCAB_ZIP')
