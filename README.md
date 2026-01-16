# Mapping-Query-Creator
Solution to create SQL queries based on YAML mappings to minimise drift being documentation and implementation.

Note: this is for simple mappings only

## Mapping

The mapping is done via yaml files, following the structure of /mapping/mapping_template.yaml.

### Transformations

The supported transformations are as follows, if an unsupported transformation is defined in the mapping an error will be thrown saying as such.

- name: concat | description: "Concatenate multiple fields with a separator" |parameters: separator: "Default is space"
- name: multiply | description: "Multiply numeric field by a factor" | parameters: factor: "Default is 1"
- name: uppercase | description: "Convert text to uppercase" | parameters: {}