from django.core.files.storage import FileSystemStorage
from storages.backends.s3boto3 import S3Boto3Storage
from storages.backends.azure_storage import AzureStorage
from storages.backends.sftpstorage import SFTPStorage
from botocore.client import Config as BotoConfig  # Aliased import
from pathlib import Path
from django.conf import settings
import os
import re
from sql.models import Config


def get_sys_config():
    all_config = Config.objects.all().values("item", "value")
    sys_config = {}
    for items in all_config:
        sys_config[items["item"]] = items["value"]
    return sys_config


class DynamicStorage:
    """动态存储适配器，根据配置选择实际存储后端"""

    def __init__(self, storage_type=None, config_dict=None):
        """根据存储服务进行文件的上传下载"""

        # 获取系统配置
        self.config = config_dict or get_sys_config()

        # 存储类型
        self.storage_type = self.config["storage_type"]

        # 本地存储相关配置信息
        self.local_path = "downloads/DataExportFile/"

        # SFTP 存储相关配置信息
        self.sftp_host = self.config["sftp_host"]
        self.sftp_user = self.config["sftp_user"]
        self.sftp_password = self.config["sftp_password"]
        self.sftp_port = int(self.config.get("sftp_port", 22))
        self.sftp_path = self.config["sftp_path"]

        # AWS S3 存储相关配置信息
        self.s3_access_key = self.config.get("s3_access_key")
        self.s3_secret_key = self.config.get("s3_secret_key")
        self.s3_bucket = self.config.get("s3_bucket")
        self.s3_region = self.config.get("s3_region")
        self.s3_endpoint = self.config.get("s3_endpoint")
        self.s3_path = self.config.get("s3_path")
        self.s3_addressing_style = self.config.get("s3_addressing_style", "virtual")

        # Azure Blob 存储相关配置信息
        self.azure_account_name = self.config["azure_account_name"]
        self.azure_account_key = self.config["azure_account_key"]
        self.azure_container = self.config["azure_container"]
        self.azure_path = self.config["azure_path"]

        self.storage = self._init_storage()

    def _init_storage(self):
        """根据配置初始化存储后端"""

        if self.storage_type == "local":
            return FileSystemStorage(
                location=str(self.local_path),
                base_url=f"{self.local_path}",
            )

        elif self.storage_type == "sftp":
            return SFTPStorage(
                host=self.sftp_host,
                params={
                    "username": self.sftp_user,
                    "password": self.sftp_password,
                    "port": self.sftp_port,
                },
                root_path=self.sftp_path,
            )

        elif self.storage_type == "s3":
            # S3兼容存储，支持AWS S3和阿里云OSS等
            boto_config = BotoConfig(s3={'addressing_style': self.s3_addressing_style}) # Use aliased name
            s3_kwargs = {
                "access_key": self.s3_access_key,
                "secret_key": self.s3_secret_key,
                "bucket_name": self.s3_bucket,
                "location": self.s3_path,
                "file_overwrite": False,
                "config": boto_config,
            }
            if self.s3_endpoint:
                # 如果配置了endpoint，则用于S3兼容存储，如OSS
                s3_kwargs["endpoint_url"] = self.s3_endpoint
            else:
                # 未配置endpoint，则用于AWS S3
                s3_kwargs["region_name"] = self.s3_region
            return S3Boto3Storage(**s3_kwargs)

        elif self.storage_type == "azure":
            return AzureStorage(
                account_name=self.azure_account_name,
                account_key=self.azure_account_key,
                azure_container=self.azure_container,
                location=self.azure_path,
            )

        raise ValueError(f"不支持的存储类型: {self.storage_type}")

    # 代理存储方法
    def save(self, name, content):
        if self.storage_type == "sftp":
            with self.storage as s:  # 参考官方文档SFTPStorage 使用with as确保SFTP底层ssh连接关闭。
                return s.save(name, content)
        else:
            return self.storage.save(name, content)

    def open(self, name, mode="rb"):
        return self.storage.open(name, mode)

    def delete(self, name):
        return self.storage.delete(name)

    def exists(self, name):
        return self.storage.exists(name)

    def size(self, name):
        return self.storage.size(name)

    def close(self):
        if hasattr(self.storage, "close"):
            return self.storage.close()

    def url(self, name):
        if hasattr(self.storage, "url"):
            return self.storage.url(name)
        return f"/download/{name}"

    def check_connection(self):
        """测试存储连接是否有效"""

        if self.storage_type == "sftp":
            with self.storage as s:
                s.listdir(".")

        elif self.storage_type == "s3":
            client = self.storage.connection.meta.client
            client.head_bucket(Bucket=self.storage.bucket_name)

        elif self.storage_type == "azure":
            container_client = self.storage.client.get_container_client(
                self.storage.container_name
            )
            container_client.get_container_properties()