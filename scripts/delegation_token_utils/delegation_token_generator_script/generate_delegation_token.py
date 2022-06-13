"""
Utility to generate delegation token and store it in the CP4D vault secret
Usage: python generate_delegation_token.py <username> <apikey>
       where username and apikey are credentials of the CP4D cluster where the token is to be stored in the vault
This script does the following:
- Read input from the properties file
- Validate the required fields
- Generate HDFS token and store it in the token file location specified in the properties file
- Generate HMS token and append it to the HDFS token in the same token file
- Encode the file content in base64 format
- Check if the vault contains secret to store the delegation token. If yes, then update the existing secret with the renewed token. Otherwise create a new secret to store the token

Steps to run the script:
1. Put the script file and the delegation_token.conf file in the same directory. Update required fields mentioned in the delegation_token.conf file
2. Add required jar files to the CLASSPATH. This includes DelegationTokenFetcher.jar which generates the HMS token, hive specific jars, hadoop common, hadoop hdfs, hadoop mapreduce jars
Example: export CLASSPATH="/home/batchIAE/token_generator/DelegationTokenFetcher.jar:/home/hadoop/hive/lib/*:/home/hadoop/hadoop/share/hadoop/common/*:/home/hadoop/hadoop/share/hadoop/common/lib/*:/home/hadoop/hadoop/share/hadoop/hdfs/*:/home/hadoop/hadoop/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop/share/hadoop/mapreduce/*:/home/hadoop/hadoop/share/hadoop/mapreduce/lib/*"
3. Run the script as: python generate_delegation_token.py <username> <apikey>

"""

import base64
import configparser
import os, sys
import requests
from subprocess import run

def print_usage():
    print("\nUsage: python generate_delegation_token.py <username> <apikey>")
    print("\nWhere \n 'username' and 'apikey' are the credentials of the CP4D cluster where the vault exists in which the delegation token will be stored.\n")
    print("\nSteps to run the script:")
    print("1. Put the script file and the delegation_token.conf file in the same directory. Update required fields mentioned in the delegation_token.conf file")
    print("2. Add required jar files to the CLASSPATH. This includes DelegationTokenFetcher.jar which generates the HMS token, hive specific jars, hadoop common, hadoop hdfs, hadoop mapreduce jars")
    print('Example: export CLASSPATH="/home/batchIAE/token_generator/DelegationTokenFetcher.jar:/home/hadoop/hive/lib/*:/home/hadoop/hadoop/share/hadoop/common/*:/home/hadoop/hadoop/share/hadoop/common/lib/*:/home/hadoop/hadoop/share/hadoop/hdfs/*:/home/hadoop/hadoop/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop/share/hadoop/mapreduce/*:/home/hadoop/hadoop/share/hadoop/mapreduce/lib/*"')
    print("3. Run the script as: python generate_delegation_token.py <username> <apikey>\n")
    exit(1)

# Check command line arguments. If it does not specify credentials of the CP4D cluster, then print usage details
args_length = len(sys.argv)
if args_length < 3:
    print_usage()

USERNAME = sys.argv[1]
APIKEY = sys.argv[2]
#OPTION = sys.argv[3]

# Read inputs from the properties file
config = configparser.ConfigParser()
properties = dict()
config_file = "./delegation_token.conf"
config.read(config_file)
if config.has_section("hadoop_conf"):
    properties = config["hadoop_conf"]
else:
    raise ValueError('Details of the hadoop cluster could not be found in the {} file. Please ensure that hadoop_conf section exists and is configured with the required details.'.format(config_file))

if config.has_section("cp4d_conf"):
    properties.update(config["cp4d_conf"])
else:
    raise ValueError('Details of the CP4D cluster could not be found in the {} file. Please ensure that cp4d_conf section exists and is configured with the required details.'.format(config_file))

# Method to fetch value for given property name, if it does not exist in the properties dictionary then return default value
def get_property(property_name, default=None):
    if properties.get(property_name):
        return properties.get(property_name)
    return default

# Read the properties
HIVE_METASTORE_URI = get_property("HIVE_METASTORE_URI")
HIVE_KERBEROS_PRINCIPAL = get_property("HIVE_KERBEROS_PRINCIPAL")
KEYTAB_FILE = get_property("KEYTAB_FILE")
DELEGATION_TOKEN_FILE = get_property("DELEGATION_TOKEN_FILE")
HIVE_USER = get_property("HIVE_USER", default="hive")
# CP4D specific properties
CLUSTER_URL = get_property("CP4D_HOST")
VAULT_URN = get_property("VAULT_URN", default="0000000000:internal")
DELEGATION_TOKEN_SECRET_NAME = get_property(
    "DELEGATION_TOKEN_SECRET_NAME", default="spark-hadoop-delegation-token-details")

# Method to validate input properties
def validate_properties():
    if HIVE_METASTORE_URI is None or HIVE_METASTORE_URI == "":
        raise ValueError("Value for HIVE_METASTORE_URI is not provided or is empty.")
    if HIVE_KERBEROS_PRINCIPAL is None or HIVE_KERBEROS_PRINCIPAL == "":
        raise ValueError("Value for HIVE_KERBEROS_PRINCIPAL is not provided or is empty.")
    if KEYTAB_FILE is None or KEYTAB_FILE == "":
        raise ValueError("Value for KEYTAB_FILE is not provided or is empty.")
    if DELEGATION_TOKEN_FILE is None or DELEGATION_TOKEN_FILE == "":
        raise ValueError("Value for DELEGATION_TOKEN_FILE is not provided or is empty.")
    if CLUSTER_URL is None or CLUSTER_URL == "":
        raise ValueError("Value for CP4D_HOST is not provided or is empty.")   

# Method to generate HDFS token
def fetch_hdfs_token():
    # clean token cache
    run("kdestroy", shell=True)
    # Clear the token file
    run("rm -f {}".format(DELEGATION_TOKEN_FILE), shell=True)
    # Generating kerberos tgt
    run(["kinit", "-kt", KEYTAB_FILE, HIVE_KERBEROS_PRINCIPAL], check=True)
    print("\nGenerated Kerberos TGT.")
    run("hdfs fetchdt --renewer hdfs {}".format(DELEGATION_TOKEN_FILE), shell=True, check=True)
    print('\nGenerated HDFS token and is stored in {}.'.format(DELEGATION_TOKEN_FILE))
    run("kdestroy", shell=True)

# Method to generate the HMS token
def fetch_hms_token():
    # Run the utility to generate HMS delegation token. This needs required jars to be added to the CLASSPATH as mentioned in the usage details
    cmd = "java -cp .:{} org.apache.hadoop.security.token.delegation.DelegationTokenFetcher --fetch".format(
        os.environ.get("CLASSPATH"))
    run(cmd, shell=True, check=True)
    print("\nGenerated HMS token and appended to the HDFS token in {}.".format(DELEGATION_TOKEN_FILE))

# Method to generate IAM token for the CP4D cluster to interact with the zen vault
def get_icp_token():
    headers = {}
    headers['Accept'] = "application/json"
    headers['username'] = USERNAME
    headers['apikey'] = APIKEY
    
    token_url = "{}/v1/preauth/validateAuth".format(CLUSTER_URL)
    response = requests.get(
        token_url, headers=headers, verify=False)
    if response.ok is False:
        raise Exception("An error occurred during token generation. Status code: {}; Error message: {}".format(response.status_code, response.text))
    token = response.json().get("accessToken")
    return token

AUTH_TOKEN = get_icp_token()

def get_headers():
    headers = {}
    headers["Content-Type"] = "application/json"
    headers["Accept"] = "application/json"
    headers["Authorization"] = "Bearer " + AUTH_TOKEN
    return headers

# Method to fetch list of secrets in the vault
def get_secrets_in_vault():
    list_secrets_url = "{}/zen-data/v2/secrets".format(CLUSTER_URL)
    response = requests.get(list_secrets_url, headers=get_headers(), verify=False)
    if response.ok is False:
        print("An error occurred while listing secrets in zen vault. Status code: {}; Error message: {}".format(response.status_code, response.text))
        return None
    return response.json()

# Method to create a new secret in vault to store the secret information
def store_secret_in_vault(payload: dict):
    secrets_url = "{}/zen-data/v2/secrets".format(CLUSTER_URL)
    response = requests.post(secrets_url, json=payload, headers=get_headers(), verify=False)
    if response.ok is False:
        raise Exception("An error occurred while storing delegation token in zen vault. Status code: {}; Error message: {}".format(response.status_code, response.text))
    return response.json()

# Method to update an existing secret with the given urn in vault with the updated information
def update_secret_in_vault(secret_urn: str, payload: dict):
    secrets_url = "{}/zen-data/v2/secrets/{}".format(CLUSTER_URL, secret_urn)
    response = requests.patch(secrets_url, json=payload, headers=get_headers(), verify=False)
    if response.ok is False:
        raise Exception("An error occurred while updating delegation token in zen vault. Status code: {}; Error message: {}".format(response.status_code, response.text))
    return response.json()

# The script execution workflow
def execute_script():
    """
    - Validate the input properties
    - Generate HDFS token and store it in the token file location specified in the properties file
    - Generate HMS token and append it to the HDFS token in the same token file
    - Read the token file content in base64 format string. This is the delegation token to be stored in the vault
    - Check if the vault contains a secret to store the delegation token. If yes, then update the existing secret with the renewed token. Otherwise create a new secret to store the token
    """
    # Validate properties 
    validate_properties()

    # Fetch HDFS and HMS token
    fetch_hdfs_token()
    fetch_hms_token()
    
    delegation_token = None
    # Get the delegation token in base64 encoded form
    with open(DELEGATION_TOKEN_FILE, "rb") as f:
        file_content = f.read()
        delegation_token = base64.b64encode(file_content).decode('ascii').replace("\n", "")
    #print(delegation_token)
    # Store the delegation token details as a secret in vault
    secrets = get_secrets_in_vault()
    # Check if secret with given name already exists in the vault. If yes, update it otherwise create a new secret
    secret_exists = False
    secret_urn = None
    if secrets:
        for secret in secrets.get("secrets"):
            if secret and secret.get("vault_urn") == VAULT_URN and secret.get("secret_name") == DELEGATION_TOKEN_SECRET_NAME:
                secret_exists = True
                secret_urn = secret.get("secret_urn")
                break
    if secret_exists:
        print("\nStoring delegation token details in existing vault secret with secret_urn {}.".format(secret_urn))
        # Prepare payload
        update_secret_payload = {
            "secret": {
                "generic": {
                    "ae.spark.remoteHadoop.isSecure": "true",
                    "ae.spark.remoteHadoop.services": "HDFS,HMS",
                    "spark.hadoop.hive.metastore.kerberos.principal": HIVE_KERBEROS_PRINCIPAL,
                    "spark.hadoop.hive.metastore.uris": HIVE_METASTORE_URI,
                    "ae.spark.remoteHadoop.delegationToken": delegation_token
                }
            }
        }
        update_secret_in_vault(secret_urn, update_secret_payload)
        print("\nUpdated existing secret with secret_urn {} in the vault with the renewed token.".format(secret_urn))
    else:
        print("\nCreating new secret in vault to store the delegation token details.")
        create_secret_payload = {
            "secret_name": DELEGATION_TOKEN_SECRET_NAME,
            "description": "Spark Hadoop Delegation Token to communicate with Kerberos secured Hive",
            "secret": {
                "generic": {
                    "ae.spark.remoteHadoop.isSecure": "true",
                    "ae.spark.remoteHadoop.services": "HDFS,HMS",
                    "spark.hadoop.hive.metastore.kerberos.principal": HIVE_KERBEROS_PRINCIPAL,
                    "spark.hadoop.hive.metastore.uris": HIVE_METASTORE_URI,
                    "ae.spark.remoteHadoop.delegationToken": delegation_token
                }
            },
            "type": "generic",
            "vault_urn": VAULT_URN
        }
        create_secret_response = store_secret_in_vault(create_secret_payload)
        print("\nCreated new secret in vault to store the delegation token details. Secret urn: {}".format(create_secret_response.get("secret_urn")))

# Invoke the method which executes the script workflow
execute_script()
