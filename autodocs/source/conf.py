import sys
import os

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
project = 'tahoe'
copyright = '2022, garg'
author = 'garg'

extensions = ['sphinx.ext.todo', 'sphinx.ext.viewcode', 'sphinx.ext.autodoc', 'sphinx.ext.autosummary']
autodoc_mock_imports = ["pandas", "deepdiff", "pydeequ", "pyspark"]
templates_path = ['_templates']
exclude_patterns = []

autosummary_generate = True
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_css_files = ['custom.css']
autosummary_generate = True
add_module_names = False


sys.path.insert(0, os.path.abspath("../../tahoe_infrastructure"))
sys.path.insert(0, os.path.abspath("../../tahoe-cli"))
sys.path.insert(0, os.path.abspath("../../base/common"))




 