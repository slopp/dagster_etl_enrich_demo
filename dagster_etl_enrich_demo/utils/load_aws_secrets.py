
import os
from typing import Optional
from dagster_aws.secretsmanager.secrets import construct_secretsmanager_client
import json

def _set_secret_if_not_set(secret_dict):
    """
    Sets an environment variable to secret_dict KEY=VALUE if it is not already set
    """

    secret_name = list(secret_dict.keys())[0]
    secret_value = secret_dict[secret_name]

    if secret_name in os.environ:
        print(f"Not setting {secret_name} from AWS Secret Manager, already set")
        return 

    os.environ[secret_name] = secret_value

    return

def load_aws_secret(secret_name, region_name: Optional[str] = None, profile_name: Optional[str] = None):
    """
    Attempts to load `secret_name` from AWS Secret Manager
    If the secret is a binary or simple string, an environment variable
    is created with the secret name and secret value 
    If the secret is a json string, an environment variable is 
    created with the secret name and value equal to the secret's KEY and VALUE
    Does NOT overwrite existing environment variables
    Logs a message but does not fail if the secret can not be found
    """
    secrets = construct_secretsmanager_client(max_attempts = 1, region_name = region_name, profile_name = profile_name)

    try:
        secret = secrets.get_secret_value(SecretId=secret_name)
    except Exception as err: 
        print(f"Failed to find {secret_name} with AWS Secret Manager, error: {err}")
        return

    if secret is None:
        print(f"{secret_name} not found in AWS Secret Manager")
        return 

    secret_dict = {}

    if 'SecretBinary' in secret.keys():
        secret_dict[secret_name] = secret['SecretBinary']
        _set_secret_if_not_set(secret_dict)
        return
    
    if 'SecretString' in secret.keys():
        try:
            secret_dict = json.loads(secret['SecretString'])
        except JSONDecodeError:
            secret_dict[secret_name] =  secret['SecretString']
        _set_secret_if_not_set(secret_dict)
        return

    print(f"Found but not able to parse secret for {secret_name}")
    return
