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

from basicauth import decode

from service.exception.exceptions import AuthenticationError
from service.utils.environment import Environment
from service.utils.kerberos_util import KerberosUtil
from service.utils.sw_logger import SwLogger

logger = SwLogger(__name__)


class Auth:

    def __init__(self, request_headers):
        self.basic_token = request_headers.get("Authorization")
        self.validate_input()

    def validate_input(self):
        if self.basic_token is None:
            raise AuthenticationError("Missing Authorization token")

    def authenticate(self):
        """
        Checks whether the user is valid.
        """
        try:
            auth_header = self.basic_token
            username, password = decode(auth_header)

            user_principal = None
            allowlisted_users = Environment().get_allowlisted_users()
            if allowlisted_users is not None:
                password_from_allowlist = allowlisted_users.get(username)
                if password_from_allowlist is None or password_from_allowlist != password:
                    logger.log_error("Invalid user credentials provided")
                    raise AuthenticationError("Invalid user credential")
            else:
                raise AuthenticationError("No whitelisted users found to authenticate against")

            if Environment().is_kerberos_enabled():
                user_principal = self.get_user_principal(username)
                key_tab_path = Environment().get_hdfs_keytab_file_path()
                logger.log_info("Minting a kerberos ticket for principal {} using keytab {}".format(user_principal, key_tab_path))
                if key_tab_path is None or user_principal is None:
                    raise AuthenticationError("Keytab file or kerberos principal missing")
                returncode = KerberosUtil.renew_kinit(key_tab_path, user_principal)
                logger.log_info('kinit return code:' + str(returncode))

            return username, user_principal
        except Exception as e:
            logger.log_exception("Failed while authenticating user", exc_info=True)
            raise AuthenticationError(str(e))

    @staticmethod
    def get_user_principal(username):

        principals = Environment().get_kerberos_principals()
        if principals is not None:
            return principals.get(username)
        else:
            raise AuthenticationError("No kerberized principal found for the user {}".format(username))