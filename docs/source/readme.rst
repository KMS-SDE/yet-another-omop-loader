Yet Another OMOP Loader
=======================

.. image:: https://img.shields.io/badge/license-MIT-blue.svg
   :target: https://opensource.org/licenses/MIT
   :alt: License: MIT

Description
-----------
Yet Another OMOP Loader is a tool designed to load data into an OMOP Common Data Model Postgres database. 
It includes functionality to check the state of the database and handle various command line arguments to support pipeline operations.

Individual operations are designed to be idempotant and can be run in any order. 
The script will check the state of the database and only run the operations that are necessary to bring the database up to date.

.. |RTD| replace:: **Read the docs »**
.. _RTD: https://yet-another-omop-loader.readthedocs.io/en/latest/

|RTD|_

Installation
------------
To install Yet Another OMOP Loader:

.. code-block:: bash

    git clone https://github.com/elementechemlyn/yet-another-omop-loader/

    pip install -r requirements.txt

Usage
-----
Here are some examples of how to use the tool:

.. code-block:: bash

    # Display help message
    python omoploader.py --help

    # Run in debug mode
    python omoploader.py --debug all

    # Perform a dry run (rollback transaction on completion)
    python omoploader.py --dryrun all

    # Skip checking the state of the database before running action
    python omoploader.py --skipcheck all

    # Clean the database by removing all objects
    python omoploader.py clean

    # Build the CDM Tables
    python omoploader.py build

    # Load the Vocabularies
    python omoploader.py vocabs

    # Load the CSV data
    python omoploader.py load

    # Build the primary keys
    python omoploader.py pkeys

    # Build the indexes
    python omoploader.py index

    # Build the foreign keys
    python omoploader.py fkeys

    # Run all actions except for clean
    python omoploader.py all

    # Reload the CSV data
    python omoploader.py reload

TODO
----
- Add support for additional database types
- Add support for different schema for data and vocabs
- Add support for dropping all constraints
- Add support for re-building constraints after reloading data
- Add support for running the Data Quality Dashboard checks

Contributing
------------
We welcome contributions to the Yet Another OMOP Loader project. To contribute, please follow these steps:

1. Fork the repository on GitHub.
2. Create a new branch for your feature or bugfix (`git checkout -b feature-branch`).
3. Make your changes and commit them (`git commit -am 'Add new feature'`).
4. Push your changes to your fork (`git push origin feature-branch`).
5. Create a pull request on the main repository.

License
-------
This project is licensed under the MIT License. See the LICENSE file for more details.

Contact
-------
For any questions or issues, please contact the project maintainers:

- GitHub: https://github.com/elementechemlyn/yet-another-omop-loader/