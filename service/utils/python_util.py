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


def get(obj: dict, path, default=None):
    """Gets the deep nested value from a dictionary

    Arguments:
        obj {dict} -- Dictionary to retrieve the value from
        path {list|str} -- List or . delimited string of path describing path.

    Keyword Arguments:
        default {mixed} -- default value to return if path does not exist (default: {None})

    Returns:
        mixed -- Value of obj at path
    """
    if isinstance(path, str):
        path = path.split(".")

    new_obj = {
        **obj
    }
    for key in path:
        if not new_obj:
            # for cases where key has null/none value
            return default

        if key in new_obj.keys():
            new_obj = new_obj.get(key)
        else:
            return default
    return new_obj


def cleanup_null_values(json_obj):
    for key in list(json_obj.keys()):
        if json_obj[key] is None:
            del json_obj[key]
        elif type(json_obj[key]) == dict:
            cleanup_null_values(json_obj[key])
