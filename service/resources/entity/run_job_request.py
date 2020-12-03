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

import json

from marshmallow import Schema, fields, post_load


class RunJobRequest:
    def __init__(self, file, proxyUser=None, className=None, args=None, jars=None, pyFiles=None, files=None,
                 driverMemory=None, driverCores=None, executorMemory=None, executorCores=None, numExecutors=None,
                 archives=None, queue=None, name=None, conf=None):
        self.file = file
        self.proxyUser = proxyUser
        self.className = className
        self.args = args
        self.jars = jars
        self.pyFiles = pyFiles
        self.files = files
        self.driverMemory = driverMemory
        self.driverCores = driverCores
        self.executorMemory = executorMemory
        self.executorCores = executorCores
        self.numExecutors = numExecutors
        self.archives = archives
        self.queue = queue
        self.name = name
        self.conf = conf

    def json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=None)

    def get(self):
        return json.loads(self.json())


class RunJobRequestSchema(Schema):
    """Class to serialize and deserialize the RunJobRequest."""
    file = fields.String()
    proxyUser = fields.String()
    className = fields.String()
    args = fields.List(fields.String())
    jars = fields.List(fields.String())
    pyFiles = fields.List(fields.String())
    files = fields.List(fields.String())
    driverMemory = fields.String()
    driverCores = fields.Integer()
    executorMemory = fields.String()
    executorCores = fields.Integer()
    numExecutors = fields.Integer()
    archives = fields.List(fields.String())
    queue = fields.String()
    name = fields.String()
    conf = fields.Raw()

    @post_load
    def create_run_job_request(self, data):
        return RunJobRequest(**data)
