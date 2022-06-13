import base64
import configparser
import os, sys
import requests
from subprocess import run

# Read inputs from the properties file
config = configparser.ConfigParser()
properties = dict()
config_file = "./delegation_token.conf"
config.read(config_file)
if config.has_section("hadoop_conf"):
    properties = config["hadoop_conf"]
else:
    raise ValueError('Details of the hadoop cluster could not be found in the {} file. Please ensure that hadoop_conf section exists and is configured with the required details.'.format(config_file))


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
CLASSPATH = get_property("CLASSPATH")

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

# Method to generate HDFS token
def fetch_hdfs_token():
    # clean token cache
    run("kdestroy", shell=True)
    # Clear the token file
    run("rm -f {}".format(DELEGATION_TOKEN_FILE), shell=True)
    # Generating kerberos tgt
    run(["kinit", "-kt", KEYTAB_FILE, HIVE_KERBEROS_PRINCIPAL], check=True)
    print("\nGenerated Kerberos TGT.")
    run("hdfs fetchdt --renewer {} {}".format(HIVE_USER, DELEGATION_TOKEN_FILE), shell=True, check=True)
    print('\nGenerated HDFS token and is stored in {}.'.format(DELEGATION_TOKEN_FILE))
    run("kdestroy", shell=True)

# Method to generate the HMS token
def fetch_hms_token():
    # Run the utility to generate HMS delegation token. This needs required jars to be added to the CLASSPATH as mentioned in the usage details
    cmd = "java -cp .:{} org.apache.hadoop.security.token.delegation.DelegationTokenFetcher --fetch".format(CLASSPATH)
    run(cmd, shell=True, check=True)
    print("\nGenerated HMS token and appended to the HDFS token in {}.".format(DELEGATION_TOKEN_FILE))

# The script execution workflow
def get_delegation_token():
    """
    - Validate the input properties
    - Generate HDFS token and store it in the token file location specified in the properties file
    - Generate HMS token and append it to the HDFS token in the same token file
    - Read the token file content in base64 format string. This is the delegation token to be stored in the vault
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
    print(delegation_token)
    return delegation_token

