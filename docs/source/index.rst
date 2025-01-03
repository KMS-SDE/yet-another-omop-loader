.. YAOL documentation master file, created by
   sphinx-quickstart on Thu Dec 26 18:08:14 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Yet Another Omop Loader
=======================

.. contents::
   :depth: 2
   :backlinks: top

Introduction
------------

Yet Another OMOP Loader is a tool designed to load data into an OMOP Common Data Model Postgres database. 

The tool is designed to be run at multiple stages of a pipeline, with the following stages.
    #. Create the database schemas and tables
    #. Load the vocabularies
    #. Load the data
    #. Create the primary keys
    #. Create the indicies
    #. Create the foreign keys

Each stage is idempotent so can be re-run to ensure a database is in a known state, the tool will not attempt to recreate objects that already exist or load data into tables which already contain data.
Seperate schemas can be specified for data and vocabularies so a single database can hold multiple OMOP datasets which share commmon vocabularies.

To configure your environment ready to use the tool:
    #. Create an empty database and a user with admin access. Set the database name and user name into :py:data:`config.DB_CONN_STR`
    #. Download the latest DDL files for postgres from https://github.com/OHDSI/CommonDataModel/tree/main/inst/ddl/5.4
    #. Set :py:data:`config.DDL_FILE`, :py:data:`config.CONSTRAINTS_FILE`, :py:data:`config.INDICIES_FILE` and :py:data:`config.KEYS_FILE` to point to the relevant files.
    #. Prepare your data into CSV files. It should be one file per CDM table. If the filename doesn't match the table name (with a .csv suffix) you can set :py:data:`config.DATA_PATTERN` to extact the table name from the file name.
    #. Set :py:data:`config.DAT_PATH` to point to your CSV files.
    #. Download a vocab zip file from Athena and set :py:data:`config.VOCABS_ZIP` to the path of the zip file.
    #. Set other environment variables as required.

Installation
------------
  | - Install the script requirements using `pip install -r requirements.txt`
  | - If you wish to rebuild the documentation also run `pip install -r requirements.docs.txt`

Usage
-----
.. argparse::
   :module: omoploader
   :func: get_args_parser
   :prog: omoploader.py

Configuration Settings
----------------------
.. autodata:: config.DDL_FILE
   :no-value:

.. autodata:: config.CONSTRAINTS_FILE
   :no-value:

.. autodata:: config.INDICIES_FILE
   :no-value:
   
.. autodata:: config.KEYS_FILE
   :no-value:
   
.. autodata:: config.DATA_PATH
   :no-value:
   
.. autodata:: config.DATA_PATTERN
   :no-value:

.. autodata:: config.DB_CONN_STR
   :no-value:

.. autodata:: config.DB_OMOP_SCHEMA
   :no-value:
   
.. autodata:: config.DB_VOCAB_SCHEMA
   :no-value:
   
.. autodata:: config.DB_RESULTS_SCHEMA
   :no-value:
   
.. autodata:: config.VOCABS_ZIP
   :no-value:

Functions
---------
.. automodule:: omoploader
   :members:

.. automodule:: dbutils
   :members:
