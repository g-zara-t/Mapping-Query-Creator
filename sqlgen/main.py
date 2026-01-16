from classes import TableMapping
from classes import fieldMapping
from classes import join
from classes import Orchestrator
from classes import yaml_read

file_path = input('Input the filepath for your yaml ')

mapping = yaml_read(file_path).normalize_mapping()
Orchestrator(mapping).build_sql()