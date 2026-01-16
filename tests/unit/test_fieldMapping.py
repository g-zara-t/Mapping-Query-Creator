from sqlgen.classes import fieldMapping

### Step 1: Create data for the test

test_mapping = [{'target': 'full_name', 'target_type': 'varchar(100)', 
                'sources': ['Customer.first_name', 'Customer.last_name'], 
                'transformations': {'type': 'concat', 'separator': ' '}}]

def test_fieldMapping(test_data):
    logic_result = fieldMapping(test_data).apply_transformations()
    assert logic_result == "CONCAT_WS(' ', Customer.first_name, Customer.last_name) AS full_name" 