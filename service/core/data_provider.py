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

from pyhive import hive

from service.exception.exceptions import ServiceError
from service.utils.environment import Environment
from service.utils.sw_logger import SwLogger
from service.utils.constants import RecordType

logger = SwLogger(__name__)


class DataProvider:

    def __init__(self, record_type, deployment_id):
        self.record_type = RecordType(record_type)
        self.env = Environment()
        config = self.env.get_deployment_config(deployment_id)
        self.database = config.get("database")
        self.schema = config.get("schema")
        self.table = config.get(self.record_type.value+"_table")

    def get_records(self, search_filter, column_filter, order_by, limit, offset):
        conn = None
        cursor = None
        try:
            conn = hive.connect(host=self.env.get_db_hostname(),
                                database=self.database)
            logger.log_debug("Created connection")
            cursor = conn.cursor()

            fields, fields_types = self.__get_fields_types(
                cursor, self.table, column_filter)

            query = self.__get_query(column_filter=column_filter, table=self.table,
                                     search_filter=search_filter, order_by=order_by, limit=limit, offset=offset)
            logger.log_info("Executing query: " + query)
            cursor.execute(query)
            rows = cursor.fetchall()

            values = self.__get_values(
                rows=rows, fields=fields, fields_types=fields_types)

            return {"fields": fields, "values": values}
        except Exception as e:
            logger.log_error(
                "Failed while fetching data from database with error : " + str(e))
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def __get_values(self, rows, fields, fields_types):
        values = []
        for row in rows:
            value = list(row)
            for i in range(len(value)):
                if fields_types.get(fields[i]) == "timestamp":
                    value[i] = value[i].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                if fields_types.get(fields[i]) == "binary":
                    value[i] = value[i].decode("utf-8")
            values.append(value)
        return values

    def __get_query(self, column_filter, table, search_filter, order_by, limit, offset):
        if column_filter:
            query = "SELECT " + column_filter + " FROM " + table
        else:
            query = "SELECT * FROM " + table

        if search_filter:
            filters = search_filter.split(",")
            conditions = []
            for f in filters:
                fs = f.split(":")
                if fs[1] == "eq":
                    conditions.append("{}='{}'".format(fs[0], fs[2]))
                elif fs[1] == "in":
                    values = ",".join(["'{}'".format(x)
                                       for x in fs[2].split(",")])
                    conditions.append("{} IN ({})".format(fs[0], values))

            if conditions:
                query = "{} WHERE {}".format(query, " AND ".join(conditions))

        if order_by:
            fs = order_by.split(":")
            query = "{} ORDER BY {} {}".format(query, fs[0], fs[1].upper())

        limit = min(limit, 100)
        return "{} LIMIT {} OFFSET {}".format(query, limit, offset)

    def __get_fields_types(self, cursor, table, column_filter):
        cursor.execute("describe {}".format(table))
        columns = cursor.fetchall()
        fields_types = {c[0]: c[1] for c in columns}
        fields = [c[0] for c in columns]

        if column_filter:
            fields = column_filter.split(",")

        return fields, fields_types
