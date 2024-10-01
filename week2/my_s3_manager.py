import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


class S3Manager:
    """Interact with AWS s3 resource"""

    def __init__(self) -> None:
        
        self.s3_client = self._create_s3_client()

    def _create_s3_client(self) -> object:
        """Creates a AWS s3 client object

        Returns:
        Object : returns a AWS s3 client object
        """
        try:
            s3_client = boto3.client('s3')
            logging.info(
                f"Successfully connected to AWS s3 resource"
            )
            return s3_client

        except ClientError as e:
            logging.error(f"Error connecting to AWS: {e}")
            raise e
            
    def create_s3_bucket(self,bucket_name,region=None) -> bool:
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created by default in the region (us-east-1).
        Args:
            bucket_name: Bucket to create
            region: String region to create bucket in, e.g., 'us-west-2'
        Returns: 
        bool: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
                logging.info(f"Bucket:{bucket_name} created successfully")
            else:
                self.s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
                logging.info(f"Bucket:{bucket_name} created successfully")
        except ClientError as e:
            logging.error(f"Error creating bucket:{bucket_name} :{e}")
            return False
        return True
    
    

    def list_buckets(self)-> None:
        """ Retrieve the list of existing buckets """
        
        response = self.s3_client.list_buckets()

        # Output the bucket names
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print('\t', bucket["Name"])
    
    
    def upload_file(self,file_name, bucket, object_key=None) -> bool:
        """Upload a file to an S3 bucket

        Args:
        file_name: File to upload
        bucket: Bucket to upload to
        object_key: S3 object key. If not specified then file_name is used
        Returns: 
        bool: True if file was uploaded, else False
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
            logging.info(f"File: {file_name} successfully uploaded to bucket: {bucket}")
        except ClientError as e:
            logging.error(f"issue with uploading File {file_name}: {e}")
            return False
        return True
    
    
    
    def delete_object(self,region, bucket_name, object_key)-> None:
        """Delete a given object from an S3 bucket
        
        Args:
        region: bucket region
        bucket_name: name of the bucket
        object_key: object identifier in s3 bucket
        """
        try:
            response = self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            logging.info(f"Object {object_key} deleted successfully")

        except Exception as e:
            logging.error(f"Error trying to delete object: {object_key} : {e}")

    def delete_bucket(self,region, bucket_name) -> None:
        """Delete a given S3 bucket

        Args:
        region : region of the bucket
        bucket_name: name of of the bucket
        """
  
        # first delete all the objects from a bucket, if any objects exist
        response = self.s3_client.list_objects_v2(Bucket=bucket_name)
        if response['KeyCount'] != 0:
            for content in response['Contents']:
                object_key = content['Key']
                logging.info('\t Deleting object...', object_key)
                self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)


        # delete the bucket
        logging.info('\t Deleting bucket...', bucket_name)
        try:
            response = self.s3_client.delete_bucket(Bucket=bucket_name)
            logging.info(f"Bucket:{bucket_name} deleted successfully")
        except Exception as e:
            logging.error(f"Error deletiing bucket:{bucket_name}: {e}")
    
    def download_s3_object(self,bucket,object_key,destination_file_name) -> None:
        """Download an Object to filesystem
        
        Args:
        bucket: bucket name
        object: object identifier in s3 bucket
        destination_file_name: filename or path of downloaded file
        """
        try:
            self.s3_client.download_file(Bucket=bucket, Key=object_key, Filename=destination_file_name)
            logging.info(f"{object_key} downloaded successfully...")
        except Exception as e:
            logging.error(f"Error downloading file:{object_key} :{e}")


    def enable_versioning_on_bucket(self,bucket) -> None:
        """Version an S3 bucket 
        
        Args;
        bucket: name of bucket in S3
        """

        try:
            response = self.s3_client.put_bucket_versioning(
                Bucket=bucket,
                VersioningConfiguration={
                    'Status': 'Enabled'
                        },
                )
            logging.info(f"Versioning applied to bucket:{bucket}")
            print(response)
        except Exception as e:
            logging.error(f"Error versioning bucket: {bucket}: {e}")
        
    
    def list_objects_in_bucket(self,bucket) -> list:
        """list objects in s3 bucket
        
        Args:
        bucket: name of the bucket
        
        Returns:
        list : list of buckets in object
        """
        
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket)
            logging.info(f"{len(response.get("Contents",[]))} objects found in bucket:{bucket}")
            if response["KeyCount"] != 0:
                for index,content in enumerate(response["Contents"], start=1):
                    object_key = content["Key"]
                    print(f"{index}:{object_key}")
            
            else:
                print(f"The bucket:{bucket} is empty")
            return response.get('Contents',[])
        except Exception as e:
            logging.error(f"Error returning objects in bucket:{bucket} : {e}")
            return []
        