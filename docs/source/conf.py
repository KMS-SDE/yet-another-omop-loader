import os
import sys
# Configuration file for the Sphinx documentation builder.

sys.path.insert(0,os.path.abspath("../.."))

project = 'YAOL'
copyright = '2024, elementechemlyn'
author = 'elementechemlyn'
release = '0.1'

extensions = ["sphinx.ext.autodoc",'sphinxarg.ext','sphinx_readme',]

templates_path = ['_templates']
exclude_patterns = []

html_theme = 'alabaster'
html_static_path = ['_static']

html_theme_options = {
    # Disable showing the sidebar. Defaults to 'false'
    'nosidebar': True,
}

autodoc_typehints = 'none'

html_context = {
   'display_github': True,
   'github_user': 'elementechemlyn',
   'github_repo': 'yet-another-omop-library',
}

html_baseurl = "https://sphinx-readme.readthedocs.io/en/latest"

readme_src_files = "readme.rst"

readme_docs_url_type = "code"