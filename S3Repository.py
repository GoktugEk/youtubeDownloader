import io
import os

import pathlib
import boto3
from dotenv import dotenv_values

__version__ = "1"

from tqdm import tqdm


class S3Repository:
    def __init__(self,
                 bucket_name: str,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_region=None):

        if aws_access_key_id is None or aws_secret_access_key is None:
            self.session = self.get_session_from_env()
        else:
            self.session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                region_name=aws_region or "us-west-1",
                aws_secret_access_key=aws_secret_access_key
            )

        self.client = self.session.client("s3")
        self.bucket_name = bucket_name

    def bucket(self):
        return self.session.resource("s3").Bucket(self.bucket_name)

    def filter(self, prefix):
        return self.bucket().objects.filter(Prefix=prefix)

    def copy(self, from_path, to_path):
        input_source = {'Bucket': self.bucket_name, "Key": from_path}
        return self.session.resource("s3") \
            .Object(self.bucket_name, to_path) \
            .copy_from(Bucket=self.bucket_name, CopySource=input_source)

    def move(self, from_path, to_path):
        self.copy(from_path, to_path)
        self.delete(from_path)

    def __iter__(self):
        return iter(self.bucket().objects.all())

    def __len__(self):
        return len(list(self.__iter__()))

    def download(self, key):
        bytes_buffer = io.BytesIO()
        self.client.download_fileobj(Bucket=self.bucket_name, Key=key, Fileobj=bytes_buffer)
        return bytes_buffer

    def get_presigned_url(self, key, expiration: int = 3600):
        presigned_url = self.client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket_name,
                "Key": key
            },
            ExpiresIn=expiration
        )

        return presigned_url

    def put_object(self, key, body):
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType="image/PNG"
        )

    def put_file(self, key, path):
        self.client.upload_file(
            path,
            self.bucket_name,
            key
        )

    def iter_objects(self):

        for obj in self.bucket().objects.all():
            if obj.key.endswith("/"):
                continue
            else:
                yield obj

    def purge(self):
        self.bucket().objects.all().delete()

    def get_session_from_env(self):
        aws_access_key_id, aws_secret_access_key, region_name = self.get_credentials_from_env()

        if aws_access_key_id is None or aws_secret_access_key is None or region_name is None:
            raise Exception(
                f"Session cannot be created with given credentials.\nAWS_ACCESS_KEY_ID={aws_access_key_id}\nAWS_SECRET_ACCESS_KEY={aws_secret_access_key}\nAWS_REGION_NAME={region_name}\n")

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            region_name=region_name,
            aws_secret_access_key=aws_secret_access_key
        )
        return session

    @staticmethod
    def get_credentials_from_env():
        dirname = os.path.dirname(os.path.abspath(__file__))
        dotenv_path = os.path.join(dirname, ".env")
        config = dotenv_values(dotenv_path=dotenv_path)
        aws_access_key_id = config.get("AWS_ACCESS_KEY_ID", None)
        aws_secret_access_key = config.get("AWS_SECRET_ACCESS_KEY", None)
        region_name = config.get("AWS_REGION", "us-west-1")
        return aws_access_key_id, aws_secret_access_key, region_name

    def delete(self, prefix):
        self.bucket().object_versions.filter(Prefix=prefix).delete()

    def download_all(self, prefix="", out_dir="out"):
        pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)

        def download_to(obj):
            if obj.key.endswith("/"):
                return
            else:
                object_key = obj.key
                write_path = os.path.join(out_dir, object_key)
                write_dir = os.path.dirname(write_path)
                pathlib.Path(write_dir).mkdir(parents=True, exist_ok=True)
                bytes_array = self.download(object_key)
                with open(write_path, "wb") as fp:
                    fp.write(bytes_array.getvalue())

        all_objects = list(self.filter(prefix))
        iterator = tqdm(all_objects, total=len(all_objects), desc=f"Downloading {self.bucket_name}", unit=" object")

        for obj in iterator:
            download_to(obj)
