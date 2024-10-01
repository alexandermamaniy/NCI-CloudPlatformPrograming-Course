from dynamo_db import DynamoDb


def main():
 
    
    # TASK: Create a DynamoDB table
    region = 'us-east-1'
    table_name="music"
    
    db_client = DynamoDb()
    
    """
    key_schema=[
        {
            "AttributeName": "artist",
            "KeyType": "HASH"
        },
        {
            'AttributeName': 'song',
            'KeyType': 'RANGE'
        }
    ]
    
    attribute_definitions=[
        {
            "AttributeName": "artist",
            "AttributeType": "S"
        },
        {
            "AttributeName": "song",
            "AttributeType": "S"
        }
        
    ]
    
    provisioned_throughput={
        "ReadCapacityUnits": 1,
        "WriteCapacityUnits": 1
    }
    
    db_client.create_table(table_name, key_schema, attribute_definitions, provisioned_throughput, region)
   
    
    # TASK: Store data (items) into a DynamoDB table
    item = {
        "artist": "Pink Floyd",
        "song": "Us and Them",
        "album": "The Dark Side of the Moon",
        "year": 1973
    }
    
    #add item to table
    db_client.store_an_item(region, table_name, item)
    
    item = {
        "artist": "Michael Jackson",
        "song": "Billie Jean",
        "album": "Thriller",
        "length_seconds": 294
    }
    
    #add item to table
    db_client.store_an_item(region, table_name, item)
    
    # TASK: Retrieve the attributes of the item with the given primary key
    
    key_info={
        "artist": "Pink Floyd",
        "song": "Us and Them",
    }
    
    db_client.get_an_item(region, table_name, key_info) """
    
    key = {
        "artist" : "Michael Jackson",
        "song": "Billie Jean"
    }
    
    update_values = "SET album =  :new_ablum"
    
    expected_values = {
        ':new_ablum' : 'NCI'
    }
    
    db_client.update_an_item(table_name=table_name,key=key,update_attributes=update_values,new_attribute_values=expected_values,region=region)
if __name__ == "__main__":
    main()