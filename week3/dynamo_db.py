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