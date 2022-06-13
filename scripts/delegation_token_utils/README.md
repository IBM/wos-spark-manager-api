
# Hadoop Delegation Token Generation

  
This document explains how to generate a Hadoop delegation token which is required to communicate with Kerberized Hive from IAE Spark.

There are two ways to generate a Hadoop delegation token

1. Executing a script on the edge node of the Hadoop cluster

2. Hosting a REST API on the edge node of the Hadoop cluster


 ## Executing a script on the edge node of the Hadoop cluster

The script to generate Hadoop delegation token does the following:

- Read input from the properties file

- Validate the required fields

- Generate HDFS token and store it in the token file location specified in the properties file

- Generate HMS token and append it to the HDFS token in the same token file

- Encode the file content in base64 format

- Check if the CP4D vault contains a secret to store the delegation token. If yes, then update the existing secret with the renewed token. Otherwise create a new secret to store the token.

**Steps to run the script from edge node of the Hadoop cluster:**

- Put the script file(generate_delegation_token.py) and the delegation_token.conf file in the same directory. Update required fields mentioned in the delegation_token.conf file

- Add required jar files to the CLASSPATH. This includes DelegationTokenFetcher.jar which generates the HMS token, hive specific jars, hadoop common, hadoop hdfs, hadoop mapreduce jars

	Example: export CLASSPATH="/home/batchIAE/token_generator/DelegationTokenFetcher.jar:/home/hadoop/hive/lib/*:/home/hadoop/hadoop/share/hadoop/common/*:/home/hadoop/hadoop/share/hadoop/common/lib/*:/home/hadoop/hadoop/share/hadoop/hdfs/*:/home/hadoop/hadoop/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop/share/hadoop/mapreduce/*:/home/hadoop/hadoop/share/hadoop/mapreduce/lib/*"

- Run the script as: `python generate_delegation_token.py <username>  <apikey>`
where username and apikey are the CP4D cluster credentials.

The output of the script will provide secret_urn which needs to be provided while configuring the Hive integrated system so that OpenScale can fetch the delegation token by fetching the vault secret.

  
  

## Hosting a REST API on the edge node of the Hadoop cluster

User can host a REST API on the edge node of the Hadoop cluster which generates and returns the Hadoop delegation token.

The reference implementation for this API is provided in scripts/delegation_token_utils/delegation_token_generator_api.

- Update required fields in the delegation_token.conf

- The DelegationTokenFetcher.jar can be found under scripts/delegation_token_utils folder

- Make required changes in the flask app hadoop_delegation_token_generator_app.py as per your environment and host the API

User needs to provide the API endpoint while configuring the Hive integrated system so that OpenScale can fetch the delegation token by invoking the API.
