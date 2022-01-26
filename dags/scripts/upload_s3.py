def upload_files(path):
    """
    Function that write data to S3.
    path: directory where parquet files is in.
    """
    
    import logging
    import boto3
    import os
    from airflow.models import Variable

    #defining variables
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')
    S3_NAME_BUCKET = Variable.get('S3_NAME_BUCKET')
    S3_HOST_BUCKET = Variable.get('S3_HOST_BUCKET')

    #load s3 client

    logging.info(f'{S3_HOST_BUCKET}{S3_NAME_BUCKET}')

    #connect to s3 client using boto3 library
    s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key

    )

    #looping through every files inside parquet folder and upload to s3
    def uploadDirectory(path,bucketname):
        """
        Function to upload to s3 massive files in folder.
        path: the path where files is in
        bucketname: the s3 bucket where files will be stored
        """
        
        for root,dirs,files in os.walk(path):
            for file in files:
                logging.info(f"Writting file:{root}/{file}")
                try:
                    s3_client.upload_file(os.path.join(root,file),bucketname,root[-49:]+'/'+file)
                except FileNotFoundError:
                    print("The file was not found")
                    logging.error("The file was not found")
                    return False
                except NoCredentialsError:
                    print("Credentials not available")
                    logging.error("Credentials not available")
                    return False
                
    uploadDirectory(path,S3_NAME_BUCKET)