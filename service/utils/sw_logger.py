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

"""
Common logger class for logging messages
"""

import json
import logging
import sys
import traceback

from service.utils.date_util import DateUtil


class SwLogger:
    """
    Usage:
    # import sw logger
        from service.utils.sw_logger import SwLogger

    # Get the instance of mrm logger
        logger = SwLogger(__name__)

    # Log the message
        logger.log_info("Processing Heartbeat request")

    JSON_LOGGING_ENABLED env variable will decide if the logging has to be in JSON format or in plain text format.
    """

    def __init__(self, name, json_enabled=True):
        self.logger = self.get_logger(name)
        self.log_source = None
        self.json_enabled = json_enabled

    def get_logger(self, logger_name=None, logger_level=logging.DEBUG):
        logger = None
        try:
            logging.basicConfig(
                format="%(message)s", level=logging.INFO,)

            if logger_name is None:
                logger_name = "__Spark_Manager__"
            logger = logging.getLogger(logger_name)
            logger.setLevel(logger_level)
            logger.setFormatter(logging.Formatter(
                fmt="%(message)s"))

        except:
            pass
        return logger

    def msg_to_log(self, attributes, msg):
        if self.json_enabled:
            return json.dumps(attributes)
        else:
            return "[" + self.log_source + "]" + msg

    def log_info(self, msg, **kwargs):
        attributes = self.get_logging_attributes("INFO", **kwargs)
        attributes["message_details"] = msg
        self.logger.info(self.msg_to_log(attributes, msg))

    def log_error(self, err_msg, **kwargs):
        attributes = self.get_logging_attributes("ERROR", **kwargs)
        attributes["message_details"] = err_msg
        self.logger.error(self.msg_to_log(attributes, err_msg))

    def log_exception(self, exc_msg, **kwargs):
        attributes = self.get_logging_attributes("ERROR", **kwargs)
        attributes["message_details"] = exc_msg
        exc_info = kwargs.get("exc_info", False)
        if exc_info is True:
            type_, value_, traceback_ = sys.exc_info()
            attributes["exception"] = "".join(traceback.format_exception(
                type_, value_, traceback_))
        self.logger.error(self.msg_to_log(attributes, exc_msg))

    def log_warning(self, msg, **kwargs):
        attributes = self.get_logging_attributes("WARNING", **kwargs)
        attributes["message_details"] = msg
        exc_info = kwargs.get("exc_info", False)
        if exc_info is True:
            type_, value_, traceback_ = sys.exc_info()
            attributes["exception"] = "".join(traceback.format_exception(
                type_, value_, traceback_))
        self.logger.warning(self.msg_to_log(attributes, msg))

    def log_debug(self, msg, **kwargs):
        attributes = self.get_logging_attributes("DEBUG", **kwargs)
        if attributes.get("debug"):
            attributes["message_details"] = msg
            self.logger.debug(self.msg_to_log(attributes, msg))

    def log_critical(self, msg, **kwargs):
        attributes = self.get_logging_attributes("CRITICAL", **kwargs)
        attributes["message_details"] = msg
        self.logger.critical(self.msg_to_log(attributes, msg))

    def get_logging_attributes(self, level, **kwargs):
        attributes = {}

        attributes["component_id"] = "spark-manager"
        attributes["log_level"] = level
        attributes["timestamp"] = DateUtil.get_current_datetime()

        fn, lno, func = self.logger.findCaller(False)[0:3]
        self.log_source = fn + ":" + str(lno) + " - " + func
        attributes["filename"] = fn
        attributes["method"] = func
        attributes["line_number"] = str(lno)

        start_time = kwargs.get("start_time", None)
        if start_time:
            elapsed_time = DateUtil.current_milli_time() - start_time
            attributes["response_time"] = elapsed_time
            attributes["perf"] = True

        additional_info = kwargs.get("additional_info", None)
        if additional_info:
            attributes["additional_info"] = additional_info

        return attributes
