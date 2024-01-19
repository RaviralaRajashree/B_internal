def compare(table_schema, new_file_schema):
    # print("**********************",table_schema)
    # print("+++++++++++++++++++++++++",new_file_schema)
    track_changes = []
    table_cols = [col['column_name'] for col in table_schema]
    file_cols = [col['column_name'] for col in new_file_schema]
    track_change = {}
    col_details = []
    for file_column in new_file_schema:     
        if file_column['column_name'] not in table_cols:
            # print(file_column['column_name'])
            col_detail = {
                'column_name':file_column['column_name'],
                'data_type':file_column['data_type'],
                'primary_key':file_column['primary_key'],
                'is_unique':file_column['is_unique'],
                'is_null':file_column['is_unique'],
            }
            col_details.append(col_detail)
    if col_details:
        track_change['column_added'] = col_details
        track_changes.append(track_change)

    track_change = {}
    col_details = []
    for table_column in table_schema:
        if table_column['column_name'] not in file_cols:
            # print(table_column['column_name'])
            col_detail = {
                'column_name':table_column['column_name'],
                'data_type':table_column['data_type'],
                'primary_key':table_column['primary_key'],
                'is_unique':table_column['is_unique'],
                'is_null':table_column['is_unique'],
            }
            col_details.append(col_detail)
    if col_details:
        track_change['column_deleted'] = col_details
        track_changes.append(track_change)

    track_change = {}
    col_details = []
    for file_column in new_file_schema:    
        for table_column in table_schema:
            if file_column['column_name'] == table_column['column_name']:
                # print(file_column['column_name'],table_column['column_name'])
                if file_column['data_type'] != table_column['data_type']:
                    col_detail = {
                        'column_name':file_column['column_name'],
                        'value_changed':'data_type',
                        'old_value':table_column['data_type'],
                        'new_value':file_column['data_type']
                    }
                    col_details.append(col_detail)
                if file_column['primary_key'] != table_column['primary_key']:
                    col_details = {
                        'column_name':file_column['column_name'],
                        'value_changed':'primary_key',
                        'old_value':table_column['primary_key'],
                        'new_value':file_column['primary_key']
                    }
                    col_details.append(col_detail)
                if file_column['is_unique'] != table_column['is_unique']:
                    col_details = {
                        'column_name':file_column['column_name'],
                        'value_changed':'is_unique',
                        'old_value':table_column['is_unique'],
                        'new_value':file_column['is_unique']
                    }
                    col_details.append(col_detail)
                if file_column['is_null'] != table_column['is_null']:
                    col_details = {
                        'column_name':file_column['column_name'],
                        'value_changed':'is_null',
                        'old_value':table_column['is_null'],
                        'new_value':file_column['is_null']
                    }
                    col_details.append(col_detail)  
    if col_details:
        track_change['column_changed'] = col_details
        track_changes.append(track_change)         
    return track_changes




json1 = [
    {
        "column_name": "Date",
        "data_type": "VARCHAR(255)",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Time",
        "data_type": "VARCHAR(255)",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Item_Code",
        "data_type": "float",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Quantity_Sold",
        "data_type": "float",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "UnitSellingPriceRMB_per_K",
        "data_type": "float",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "SaleorReturn",
        "data_type": "VARCHAR(255)",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Discount",
        "data_type": "VARCHAR(255)",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    }
]

json2 = [
    {
        "column_name": "Date",
        "data_type": "VARCHAR(255)",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Time",
        "data_type": "VARCHAR(255)",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Item_Code",
        "data_type": "float",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Quantity_Sold_kilo",
        "data_type": "float",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "UnitSellingPriceRMB_per_KG",
        "data_type": "float",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "SaleorReturn",
        "data_type": "VARCHAR",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    },
    {
        "column_name": "Discount",
        "data_type": "bigint",
        "primary_key": False,
        "is_unique": False,
        "is_null": False
    }
]
# res = compare(json1,json2)
# print(res)

