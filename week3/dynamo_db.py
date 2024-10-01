import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


class DynamoDb:
    """Interact with AWS dynamoDB resource"""

    def __init__(self) -> None:
        
        self.db_client = self._create_db_client()

    def _create_db_client(self) -> object:
        """Creates a AWS dynamoDB client object

        Returns:
        Object : returns a AWS dy client object
        """
        try:
            db_client = boto3.client('dynamodb')
            logging.info(
                f"Successfully connected to AWS dynamodb resource"
            )
            return db_client

        except ClientError as e:
            logging.error(f"Error connecting to AWS: {e}")
            raise e
    
    def create_table(self, table_name, key_schema, attribute_definitions, provisioned_throughput, region):
        
        try:
            dynamodb_resource = boto3.resource("dynamodb", region_name=region)
            logging.info(f"\ncreating the table {table_name} ...")
            self.table = dynamodb_resource.create_table(TableName=table_name, KeySchema=key_schema, AttributeDefinitions=attribute_definitions,
                ProvisionedThroughput=provisioned_throughput)

            # Wait until the table exists.
            self.table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
            
        except ClientError as e:
            logging.error(e)
            return False
        return True

 
        

    def store_an_item(self, region, table_name, item):
        try:
            logging.info("\nstoring the item {item} in the table {table_name} ...")
            dynamodb_resource = boto3.resource("dynamodb", region_name=region)
            table = dynamodb_resource.Table(table_name)
            table.put_item(Item=item)
        
        except ClientError as e:
            logging.error(e)
            return False
        return True
        
        
     
    def get_an_item(self, region, table_name, key):
        try:
            logging.info("\nretrieving the item with the key {key} from the table {table_name} ...")
            dynamodb_resource = boto3.resource("dynamodb", region_name=region)
            table = dynamodb_resource.Table(table_name)
            response = table.get_item(Key=key)
            item = response['Item']
            print(item)
        
        except ClientError as e:
            logging.error(e)
            return False
        return True
        
    def update_an_item(self,table_name,key,update_attributes,new_attribute_values,region):
        try:
            dynamodb_resource = boto3.resource("dynamodb", region_name=region)
            table = dynamodb_resource.Table(table_name)
            table.update_item(TableName=table_name,Key=key,UpdateExpression=update_attributes,ExpressionAttributeValues=new_attribute_values)
            logging.info(f"updating item:{update_attributes} with values:{new_attribute_values}")
        
        except Exception as e:
            logging.error(f"Error updating table items: {e}")