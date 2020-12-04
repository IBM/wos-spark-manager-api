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


class KerberosUtil:

    @classmethod
    def renew_kinit(cls, keytab, principal, exit_on_fail=True):
        """
        Renew kerberos token from keytab

        Keyword arguments:
            keytab -- keytab file
            principal {str} -- user principal
            exit_on_fail {bool} -- exit operation on fail flag

        Returns:
            operation returncode
        """

        from subprocess import Popen, PIPE
        import sys

        kinit_args = ['/usr/bin/kinit', '-kt', keytab, principal]
        subp = Popen(kinit_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        subp.wait()
        if subp.returncode != 0:
            print(
                "Couldn't reinit from keytab! `kinit' exited with {}.\n{}".format(
                    subp.returncode,
                    str("".join(str(subp.stderr.readlines()) if subp.stderr else '')))
            )
            if exit_on_fail:
                sys.exit(subp.returncode)
            else:
                return subp.returncode
