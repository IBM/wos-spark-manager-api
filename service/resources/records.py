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

from flask import request
from flask_restplus import Namespace, Resource

from service.core.data_provider import DataProvider
from service.resources.entity.sw_model import SwModel
from service.utils.sw_logger import SwLogger

ns = Namespace("Records")
swagger_model = SwModel(ns)
logger = SwLogger(__name__)


@ns.route("/records")
class Records(Resource):

    @ns.doc(id="get", description="Downloads the records from database.")
    @ns.produces(["application/octet-stream"])
    @ns.param(name="type", description="The type of database. Supported values are hive.", _in="query", required=True, example="hive")
    @ns.param(name="host", description="Database host name.", _in="query", required=True, example="host")
    @ns.param(name="port", description="Database port.", _in="query", required=True, example="port")
    @ns.param(name="database", description="Database name.", _in="query", required=True, example="sampledb")
    @ns.param(name="schema", description="Database schema", _in="query", required=True, example="schema")
    @ns.param(name="table", description="Database table", _in="query", required=True, example="table")
    @ns.param(name="search_filter", description="The search criteria to use while querying.", _in="query", required=False, example="column1:eq:10")
    @ns.param(name="column_filter", description="The columns to be returned in the response.", _in="query", required=False, example="column1,column2")
    @ns.param(name="order_by", description="The column to be used to ordered the results and the order(ascending or descending).", _in="query", required=False, example="column1:desc")
    @ns.param(name="limit", description="The number of results to be returned. Maximum value is 100.", _in="query", required=False, example="20")
    @ns.param(name="offset", description="The offset of the results.", _in="query", required=False, example="10")
    @ns.response(200, "Records downloaded successfully.")
    @ns.response(401, "Unauthorized", swagger_model.error_container)
    @ns.response(500, "Internal Server Error", swagger_model.error_container)
    def get(self):

        database_type = request.args.get("type")
        host = request.args.get("host")
        port = request.args.get("port")
        database = request.args.get("database")
        schema = request.args.get("schema")
        table = request.args.get("table")
        search_filter = request.args.get("search_filter")
        column_filter = request.args.get("column_filter")
        order_by = request.args.get("order_by")
        limit = request.args.get("limit")
        limit = int(limit) if limit else 100
        offset = request.args.get("offset")
        offset = int(offset) if offset else 0

        return DataProvider(host=host, port=port, database=database, schema=schema, table=table).get_records(search_filter=search_filter, column_filter=column_filter, order_by=order_by, limit=limit, offset=offset)
