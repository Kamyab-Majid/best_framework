import logging
from typing import Any, Set

import pyarrow.parquet as pq
from pyarrow.lib import Table
from s3fs import S3FileSystem
import fsspec
from pathlib import Path
from fsspec.implementations.local import LocalFileSystem
from s3fs import S3FileSystem
import asyncio


class GeneralFileSystem:
    """"
    A class that represents a general file system.
    """

    def __init__ (self, protocol_1:str='file', protocol_2: str = 's3')->None:
        """_summary_

        Args:
            protocol_1 (_type_, optional): _description_. Defaults to 'file':str.
            protocol_2 (str, optional): _description_. Defaults to 's3'.
        """
        self.s3_fs = fsspec.filesystem(
            protocol=protocol_1,
            key=S3_KEY, secret=S3_SECRET
        )
        self.local = fsspec.filesystem(
            protocol=protocol_2,
        )  # asynchronous=True


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    # dotenv_path = Path('path/to/.env')
    # load_dotenv(dotenv_path=dotenv_path)
    load_dotenv()

    S3_KEY = os.getenv('S3_KEY')
    S3_SECRET = os.getenv('S3_SECRET')
    s3_storage_options = {"key": S3_KEY, "secret": S3_SECRET}
    # storage_options = {"anon": True} #for accessing  open access s3 buckets.
    fs = GeneralFileSystem(s3_storage_options=s3_storage_options)
    fs.s3_fs.ls('')  # listing all buckets in s3


{'file': {'class': 'fsspec.implementations.local.LocalFileSystem'}, 'memory': {'class': 'fsspec.implementations.memory.MemoryFileSystem'}, 'dropbox': {'class': 'dropboxdrivefs.DropboxDriveFileSystem', 'err': 'DropboxFileSystem requires "dropboxdrivefs","requests" and "dropbox" to be installed'}, 'http': {'class': 'fsspec.implementations.http.HTTPFileSystem', 'err': 'HTTPFileSystem requires "requests" and "aiohttp" to be installed'}, 'https': {'class': 'fsspec.implementations.http.HTTPFileSystem', 'err': 'HTTPFileSystem requires "requests" and "aiohttp" to be installed'}, 'zip': {'class': 'fsspec.implementations.zip.ZipFileSystem'}, 'tar': {'class': 'fsspec.implementations.tar.TarFileSystem'}, 'gcs': {'class': 'gcsfs.GCSFileSystem', 'err': 'Please install gcsfs to access Google Storage'}, 'gs': {'class': 'gcsfs.GCSFileSystem', 'err': 'Please install gcsfs to access Google Storage'}, 'gdrive': {'class': 'gdrivefs.GoogleDriveFileSystem', 'err': 'Please install gdrivefs for access to Google Drive'}, 'sftp': {'class': 'fsspec.implementations.sftp.SFTPFileSystem', 'err': 'SFTPFileSystem requires "paramiko" to be installed'}, 'ssh': {'class': 'fsspec.implementations.sftp.SFTPFileSystem', 'err': 'SFTPFileSystem requires "paramiko" to be installed'}, 'ftp': {'class': 'fsspec.implementations.ftp.FTPFileSystem'}, 'hdfs': {'class': 'fsspec.implementations.arrow.HadoopFileSystem', 'err': 'pyarrow and local java libraries required for HDFS'}, 'arrow_hdfs': {'class': 'fsspec.implementations.arrow.HadoopFileSystem', 'err': 'pyarrow and local java libraries required for HDFS'}, 'webhdfs': {'class': 'fsspec.implementations.webhdfs.WebHDFS', 'err': 'webHDFS access requires "requests" to be installed'}, 's3': {'class': 's3fs.S3FileSystem', 'err': 'Install s3fs to access S3'}, 's3a': {'class': 's3fs.S3FileSystem', 'err': 'Install s3fs to access S3'}, 'wandb': {'class': 'wandbfs.WandbFS', 'err': 'Install wandbfs to access wandb'}, 'oci': {'class': 'ocifs.OCIFileSystem', 'err': 'Install ocifs to access OCI Object Storage'}, 'asynclocal': {'class': 'morefs.asyn_local.AsyncLocalFileSystem', 'err': "Install 'morefs[asynclocalfs]' to use AsyncLocalFileSystem"}, 'adl': {'class': 'adlfs.AzureDatalakeFileSystem', 'err': 'Install adlfs to access Azure Datalake Gen1'}, 'abfs': {
    'class': 'adlfs.AzureBlobFileSystem', 'err': 'Install adlfs to access Azure Datalake Gen2 and Azure Blob Storage'}, 'az': {'class': 'adlfs.AzureBlobFileSystem', 'err': 'Install adlfs to access Azure Datalake Gen2 and Azure Blob Storage'}, 'cached': {'class': 'fsspec.implementations.cached.CachingFileSystem'}, 'blockcache': {'class': 'fsspec.implementations.cached.CachingFileSystem'}, 'filecache': {'class': 'fsspec.implementations.cached.WholeFileCacheFileSystem'}, 'simplecache': {'class': 'fsspec.implementations.cached.SimpleCacheFileSystem'}, 'dask': {'class': 'fsspec.implementations.dask.DaskWorkerFileSystem', 'err': 'Install dask distributed to access worker file system'}, 'dbfs': {'class': 'fsspec.implementations.dbfs.DatabricksFileSystem', 'err': 'Install the requests package to use the DatabricksFileSystem'}, 'github': {'class': 'fsspec.implementations.github.GithubFileSystem', 'err': 'Install the requests package to use the github FS'}, 'git': {'class': 'fsspec.implementations.git.GitFileSystem', 'err': 'Install pygit2 to browse local git repos'}, 'smb': {'class': 'fsspec.implementations.smb.SMBFileSystem', 'err': 'SMB requires "smbprotocol" or "smbprotocol[kerberos]" installed'}, 'jupyter': {'class': 'fsspec.implementations.jupyter.JupyterFileSystem', 'err': 'Jupyter FS requires requests to be installed'}, 'jlab': {'class': 'fsspec.implementations.jupyter.JupyterFileSystem', 'err': 'Jupyter FS requires requests to be installed'}, 'libarchive': {'class': 'fsspec.implementations.libarchive.LibArchiveFileSystem', 'err': 'LibArchive requires to be installed'}, 'reference': {'class': 'fsspec.implementations.reference.ReferenceFileSystem'}, 'generic': {'class': 'fsspec.generic.GenericFileSystem'}, 'oss': {'class': 'ossfs.OSSFileSystem', 'err': 'Install ossfs to access Alibaba Object Storage System'}, 'webdav': {'class': 'webdav4.fsspec.WebdavFileSystem', 'err': 'Install webdav4 to access WebDAV'}, 'dvc': {'class': 'dvc.api.DVCFileSystem', 'err': 'Install dvc to access DVCFileSystem'}, 'root': {'class': 'fsspec_xrootd.XRootDFileSystem', 'err': "Install fsspec-xrootd to access xrootd storage system. Note: 'root' is the protocol name for xrootd storage systems, not refering to root directories"}}
