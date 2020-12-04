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

from flask import Blueprint
from flask_restplus import Api

from service.exception.exceptions import (AuthenticationError, BadRequestError, ObjectNotFoundError, ServiceError,
                                          ServiceErrors, InternalServerError)
from service.resources.files import ns as fns
from service.resources.jobs import ns as jns
from service.resources.records import ns as rns
from service.utils.sw_logger import SwLogger

logger = SwLogger(__name__)

authorizations = {
    'basicAuth': {
        'type': 'basic',
        'in': 'header',
        'name': 'Authorization'
    }
}
api_blueprint = Blueprint("api", __name__)
api = Api(api_blueprint,
          version='1.0',
          title="Custom Spark Manager REST Application",
          description="Spark Manager Application that provides APIs to read and write files to remote HDFS, run and get details about a job running in remote Spark cluster.",
          doc="/spark_wrapper/api/explorer",
          authorizations=authorizations,
          security="basic_auth")
# Removing the default namespace.
api.namespaces.clear()
# Add namespace
api.add_namespace(jns, path="/openscale/spark_wrapper/v1")
api.add_namespace(fns, path="/openscale/spark_wrapper/v1")
api.add_namespace(rns, path="/openscale/spark_wrapper/v1")
"""Error handlers for handling exceptions"""


@api.errorhandler(BadRequestError)
def bad_request_handler(e):
    '''Bad request error handler'''
    logger.log_warning(str(e), exc_info=True)
    return get_error_json(e.message, e), 400


@api.errorhandler(AuthenticationError)
def authentication_error_handler(e):
    '''Authentication error handler'''
    logger.log_warning(str(e), exc_info=True)
    return get_error_json(e.message, e), 401


@api.errorhandler(ObjectNotFoundError)
def object_error_handler(e):
    '''Object not found error handler'''
    logger.log_warning(str(e), exc_info=True)
    return get_error_json(e.message, e), 404


@api.errorhandler(InternalServerError)
def internal_server_error_handler(e):
    '''Internal server error handler'''
    logger.log_exception(str(e), exc_info=True)
    return get_error_json(e.message, e), 500


@api.errorhandler(ServiceErrors)
def service_errors_handler(e):
    '''Service errors handler'''

    errors_json = []
    for err in e.errors:
        logger.log_exception(str(err), exc_info=True)

        err_message = err.message

        error_json = {
            "code": err.message_code,
            "message": err_message
        }

        if err.target:
            error_json["target"] = {
                "type": err.target.type, "name": err.target.name}

        errors_json.append(error_json)

    return {"errors": errors_json}, 500


@api.errorhandler(ServiceError)
def service_error_handler(e):
    '''Service error handler'''
    logger.log_exception(str(e), exc_info=True)
    return get_error_json("Unable to execute sw request.", e, e.message), 500


@api.errorhandler(Exception)
def exception_error_handler(e):
    '''Exception error handler'''
    logger.log_exception(str(e), exc_info=True)
    return get_error_json("Unable to handle SW request. Please check log against trace_id for exception details.", e), 500


@api.errorhandler
def default_error_handler(e):
    '''Default error handler'''
    logger.log_exception(str(e), exc_info=True)
    return get_error_json("Unable to handle SW request. Please check log against trace_id for exception details.", e), 500


def get_error_json(message, error, msg_detail=None):

    target = None
    if isinstance(error, ServiceError):
        target = error.target

    error_json = {
        "message": message if not msg_detail else msg_detail
    }

    if target:
        error_json["target"] = {"type": target.type, "name": target.name}

    error_resp = {"errors": [error_json]}

    return error_resp
