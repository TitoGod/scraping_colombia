import os
import boto3
from botocore.exceptions import NoCredentialsError

class S3Manager:
    """Class to manage all interactions with AWS S3."""
    def __init__(self, bucket_name, logger):
        self.bucket_name = bucket_name
        self.logger = logger
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )

    def upload_file(self, file_path, s3_folder):
        """Uploads a file to an S3 bucket."""
        if not os.path.exists(file_path):
            self.logger.error(f"The file '{file_path}' does not exist and cannot be uploaded to S3.")
            return False
        
        s3_key = f"{s3_folder}/{os.path.basename(file_path)}"
        self.logger.info(f"Uploading '{file_path}' to S3 bucket '{self.bucket_name}' at '{s3_key}'...")
        
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            self.logger.info(f"Successfully uploaded '{file_path}' to '{self.bucket_name}/{s3_key}'.")
            return True
        except FileNotFoundError:
            self.logger.error(f"The file '{file_path}' was not found.")
            return False
        except NoCredentialsError:
            self.logger.error("AWS credentials not found or are invalid. Please configure your environment variables.")
            return False
        except Exception as e:
            self.logger.error(f"An error occurred while uploading to S3: {e}")
            return False