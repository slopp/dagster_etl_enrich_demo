from dagster import IOManager, resource, io_manager, Field
import boto3
import os
import pandas as pd
import tempfile 

class ExcelIOManager(IOManager):
    """ Loads excel file as source asset """        
    def load_input(self, context):
        filename = context.resource_config["filename"]
        return context.resources.fs.get_excel_file(filename)

    def handle_output(self, context, obj):
        raise f"Writing type {type(obj)} to excel not yet implemented"

@io_manager(
    config_schema = {"filename": Field(str, default_value="products.xlsx") },
    required_resource_keys = {"fs"}
)
def excel_io(): 
    """ Loads excel file as source asset """
    return ExcelIOManager()

class CSVIOManager(IOManager):
    """ Loads a csv file as source asset """
    def load_input(self, context):
        filename = context.resource_config["filename"]
        return context.resources.fs.get_csv_file(filename)

    def handle_output(self, context, obj):
        raise f"Writing type {type(obj)} to csv not yet implemented"


@io_manager(
    config_schema = {"filename": Field(str, default_value="customers.csv") },
    required_resource_keys = {"fs"}
)
def csv_io():
    """ Loads a csv file as source asset """
    return CSVIOManager()

class LocalFileSystem():
    """ resource to interact with local files """
    def __init__(self, base_dir):
        self._base_dir = base_dir

    def get_mtime(self, filename):
        path = self._base_dir + "/" + filename
        return os.path.getmtime(path)
    
    def get_excel_file(self, filename):
        path = self._base_dir + "/" + filename
        return pd.read_excel(path) 
        
    def get_csv_file(self, filename):
        path = self._base_dir + "/" + filename
        return pd.read_csv(path)
            

@resource(config_schema = {"base_dir": str})
def local_fs(context):
    """ resource to interact with local files """
    base_dir = context.resource_config["base_dir"]
    return LocalFileSystem(base_dir)


class s3FileSystem():
    """ resource to interact with s3 files """
    def __init__(self, region_name, s3_bucket):
        self._region_name = region_name 
        self._s3_bucket = s3_bucket

    def get_mtime(self, filename):
        s3 = boto3.client('s3', region_name = self._region_name)
        objects = s3.list_objects(Bucket = self._s3_bucket)
        for o in objects["Contents"]:
            if o["Key"] == filename:
                mtime = o["LastModified"].timestamp() 
                return mtime

        # if we get here the file was not found                
        return None
    
    def get_excel_file(self, filename):
        s3 = boto3.client('s3', region_name = self._region_name)
        s3 = boto3.client('s3', region_name = self._region_name)
        s3.download_file(self._s3_bucket, filename, 'tmp.xlsx')
        result = pd.read_excel('tmp.xlsx')
        os.remove('tmp.xlsx')
        return result
        
        
    def get_csv_file(self, filename):
        s3 = boto3.client('s3', region_name = self._region_name)
        response = s3.get_object(Bucket = self._s3_bucket, Key = filename)
        return pd.read_csv(response.get("Body"))
                
        
@resource(config_schema = {"region_name": str, "s3_bucket": str})
def s3_fs(context):
    """ resource to interact with s3 files """
    region_name = context.resource_config["region_name"]
    s3_bucket = context.resource_config["s3_bucket"]
    return s3FileSystem(region_name, s3_bucket)


