# ----------------------------------------------------------------------------------------------------
# (C) Copyright IBM Corp. 2020.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
# ----------------------------------------------------------------------------------------------------

import configparser
import json
import os
import pathlib
from distutils.util import strtobool

from service.utils.python_util import get
from service.utils.sw_singleton import SwSingleton


class Environment(metaclass=SwSingleton):

    def __init__(self):
        config = configparser.ConfigParser()
        config.read("spark_wrapper.properties")
        self.properties = dict()
        self.kerberos_principals = None
        if config.has_section("properties"):
            self.properties = config["properties"]

        utils_dir = pathlib.Path(__file__).parent.absolute()
        with open(str(utils_dir) + "/../security/auth.json") as json_file:
            auth_json = json.load(json_file)
            self.kerberos_principals = get(auth_json, "user_kerberos_mapping")
            self.allowlisted_users = get(auth_json, "allowlisted_users")

    def get_web_hdfs_url(self):
        return self.get_property_value("WEB_HDFS_URL")

    def get_hdfs_file_base_url(self):
        return self.get_property_value("HDFS_FILE_BASE_URL")

    def get_spark_livy_url(self):
        return self.get_property_value("SPARK_LIVY_URL")

    def get_base_hdfs_location(self):
        return self.get_property_value("BASE_HDFS_LOCATION")

    def get_deployment_config(self, deployment_id):
        with open("records_mapping.json", "r") as f:
            deployments_config = json.loads(f.read())

        deployment_config = next(
            (d for d in deployments_config["deployments"] if d["deployment_id"] == deployment_id), None)
        return deployment_config

    def is_kerberos_enabled(self):
        return self.get_property_boolean_value("KERBEROS_ENABLED", "false")

    def get_hdfs_keytab_file_path(self):
        return self.get_property_value("HDFS_KEYTAB_FILE_PATH")

    def get_kerberos_principals(self):
        return self.kerberos_principals

    def get_allowlisted_users(self):
        return self.allowlisted_users

    def get_spark_yarn_keytab_file_path(self):
        return self.get_property_value("SPARK_YARN_KEYTAB_FILE_PATH")

    def get_wos_env_archive_location(self):
        return self.get_property_value("WOS_ENV_ARCHIVE_LOCATION")

    def get_wos_env_site_packages_path(self):
        return self.get_property_value("WOS_ENV_SITE_PACKAGES_PATH")

    def is_hive_client_auth_enabled(self):
        return self.get_property_boolean_value("HIVE_CLIENT_AUTHORIZATION_ENABLED", "false")

    def get_property_value(self, property_name, default=None):
        if os.environ.get(property_name):
            return os.environ.get(property_name)
        elif self.properties.get(property_name):
            return self.properties.get(property_name)
        else:
            return default

    def get_property_boolean_value(self, property_name, default=None):
        val = self.get_property_value(property_name, default)
        if val:
            # True values are y, yes, t, true, on and 1;
            # False values are n, no, f, false, off and 0
            try:
                return bool(strtobool(val))
            except ValueError:
                return False
        # return False for other values or None
        return False
