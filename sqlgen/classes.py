import yaml

class TableMapping:
    def __init__(self, target_table, fields):
        self.target_table = target_table
        self.fields = fields

    def create_sql_table(self, drop_if_exists='y'):
        fields_sql = []
        for field in self.fields:
            col_type = field.get("target_type", "VARCHAR(255)")
            fields_sql.append(f"{field['target']} {col_type}")

        fields_str = ",\n  ".join(fields_sql)

        create = f'''CREATE TABLE {self.target_table} (
  {fields_str}
);'''

        if drop_if_exists.lower() == 'y':
            drop = f'DROP TABLE IF EXISTS {self.target_table};\n'
            return drop + create

        return create


class join:
    def __init__(self, joins):
        self.joins = joins

    def create_join(self):
        for j in self.joins:
            type = j.get("type").upper()
            left_table = j.get("left")
            right_table = j.get("right")
            condition = j.get("condition")
            left_field = condition.get("left_field")
            right_field = condition.get("right_field")

        return f"""FROM 
    {left_table} 
{type} JOIN 
    {right_table}
ON
    {left_field} = {right_field}
                    """

class fieldMapping:
    def __init__(self, fields):
        self.fields = fields
        
    def apply_transformations(self):
        results = []

        for t in self.fields:
            t_type = t.get("transformations").get("type")
            args = {k: v for k, v in t.get("transformations").items() if k != "type"}

            method_name = f"transform_{t_type}"
            if not hasattr(self, method_name):
                raise ValueError(f"Transformation '{t_type}' not implemented")

            transform_method = getattr(self, method_name)
            expr = transform_method(t,**args)

            results.append(expr)

        return results

    def _format_sources(self, sources):
        """Turn a string or list into a SQL-friendly string to get rid of the pesky [ ]"""
        if isinstance(sources, list):
            return ", ".join(sources)
        return sources

    def transform_concat(self, field, separator=" "):
        sources_str = self._format_sources(field.get("sources"))
        return f"CONCAT_WS('{separator}', {sources_str}) AS {field.get('target')}"

    def transform_None(self, field):
        sources_str = self._format_sources(field.get("sources"))
        return f"{sources_str} AS {field.get('target')}"

    def transform_multiply(self, field, factor):
        sources_str = self._format_sources(field.get("sources"))
        return f"{sources_str}*{factor} AS {field.get('target')}"
                                           
    
class Orchestrator:
    def __init__(self, mapping_yaml):
        self.mapping = mapping_yaml

    def build_sql(self):
        target_table = self.mapping["target_table"]
        fields = self.mapping["fields"]
        joins = self.mapping.get("joins", [])
        sources = [s[0] for s in self.mapping.get("sources", [])]

        create_sql = TableMapping(target_table, fields).create_sql_table()
        field_sql = fieldMapping(fields).apply_transformations()
        select_sql = ",\n\t".join(field_sql)

        join_sql = join(joins).create_join() if joins else f"FROM {sources[0]}"

        insert_sql = f"""
{create_sql}
INSERT INTO {target_table} (
  {', '.join([f['target'] for f in fields])}
)
SELECT
\t{select_sql}
{join_sql}
"""
        return print(insert_sql)
    
class yaml_read:
    def __init__(self, file_name):
        self.file_name = file_name

    def ensure_list(x):
        """
        Ensure that x is a list.
        - If x is None, return []
        - If x is a list, return x
        - Otherwise, return [x]
        """
        if x is None:
            return []
        if isinstance(x, list):
            return x
        return [x]

    def normalize_mapping(self):
        '''
        Normalise the YAML Mapping file for SQL generation.

        Parameters:
        yaml_file: str
        path to the yaml mapping file

        Returns:
        dict with normalised mapping using the following structure

                {
                'target_table': str,
                'sources': {source_name: table_name, ...},
                'fields': [
                    {
                        'target': str,
                        'sources': list of str,
                        'transformations': list of dict
                    },
                    ...
                ],
                'joins': [
                    {
                        'type': str,   # join type (e.g., 'inner')
                        'left': str,   # left source name
                        'right': str,  # right source name
                        'on': list of str   # join conditions
                    },
                    ...
                ]
            }
        '''
        with open(self.file_name) as f:
            raw = yaml.safe_load(f)

        sources = {s['name']: s['table'] for s in raw.get('sources', [])}

        fields = []
        for f in raw.get('fields', []):
            src = f['source']
            if isinstance(src, str):
                src = [src]
            transforms = f.get('transform', {})
            target_type = f['target_type']
            fields.append({
                'target': f['target'],
                'target_type': target_type,
                'sources': src,
                'transformations': transforms
            })

        joins = []
        for j in raw.get('joins', []):
            on_conditions = j.get('condition')
            joins.append({
                'type': j['type'],
                'left': j['left'],
                'right': j['right'],
                'condition': on_conditions
            })

        normalized = {
            'target_table': raw['target_table'],
            'sources': sources,
            'fields': fields,
            'joins': joins
        }

        return normalized
        
