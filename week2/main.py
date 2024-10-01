from my_s3_manager import S3Manager
import argparse

def main():
    
    #create connection
    s3_client = S3Manager()

    parser = argparse.ArgumentParser(description="pass arguements to AWS API resource methods")

    #create arguments
    parser.add_argument('bucket_name', help='The name of the bucket.')
    parser.add_argument('--file_name', help='The name of the file to upload.')
    parser.add_argument('--object_key', help='The object key')
    parser.add_argument('--dest_file_name', help="destination Filepath")
    region = 'eu-west-1'
  
    args = parser.parse_args()

    #s3_client.upload_file(args.file_name,args.bucket_name, args.object_key)
    #s3_client.download_s3_object(bucket=args.bucket_name,object_key=args.object_key,destination_file_name="xcrocuses.jpg")
    #s3_client.enable_versioning_on_bucket(args.bucket_name)
    s3_client.list_objects_in_bucket(bucket="x23384069")
if __name__ == "__main__":
    main()