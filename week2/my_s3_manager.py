import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


class S3Manager:
    """Interact with AWS s3 resource"""

    def __init__(self) -> None:
        
        self.s3_client = self._create_s3_client()

    def _create_s3_client(self):
        """Creates a AWS s3 client object

        Returns:
        Object : returns a AWS s3 client object
        """
        try:
            s3_client =  s3_client = boto3.client('s3')
            logging.info(
                f"Successfully connected to AWS s3 resource"
            )
            return s3_client

        except ClientError as e:
            logging.error(f"Error connecting to AWS: {e}")
            raise e
            
    def create_s3_bucket(self,bucket_name,region=None):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created by default in the region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    

    def list_buckets(self)-> None:
        # Retrieve the list of existing buckets
        
        response = self.s3_client.list_buckets()

        # Output the bucket names
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print('\t', bucket["Name"])
    
    
    def upload_file(self,file_name, bucket, object_key=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param key: S3 object key. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 key was not specified, use file_name
        if object_key is None:
            object_key = file_name

        # Upload the file
        try:
            response = self.s3_client.upload_file(file_name, bucket, object_key)
            '''
            # an example of using the ExtraArgs optional parameter to set the ACL (access control list) value 'public-read' to the S3 object
            response = s3_client.upload_file(file_name, bucket, key, 
                ExtraArgs={'ACL': 'public-read'})
            '''
        
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    
    
    def delete_object(self,region, bucket_name, object_key):
        """Delete a given object from an S3 bucket
        """
        response = self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    


    def delete_bucket(self,region, bucket_name):
        """Delete a given S3 bucket
        """
  
        # first delete all the objects from a bucket, if any objects exist
        response = self.s3_client.list_objects_v2(Bucket=bucket_name)
        if response['KeyCount'] != 0:
            for content in response['Contents']:
                object_key = content['Key']
                print('\t Deleting object...', object_key)
                self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)


        # delete the bucket
        print('\t Deleting bucket...', bucket_name)
        response = self.s3_client.delete_bucket(Bucket=bucket_name)
        