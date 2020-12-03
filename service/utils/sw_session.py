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


class SwSession:

    def __init__(self, *kw):
        self.username = None
        self.user_principal = None

    def get_username(self):
        return self.username

    def set_username(self, username):
        self.username = username

    def get_user_principal(self):
        return self.user_principal

    def set_user_principal(self, user_principal):
        self.user_principal = user_principal
