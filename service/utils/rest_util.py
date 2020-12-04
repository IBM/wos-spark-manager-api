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

# Supress info and warning logs from requests and urllib3
import logging
from http import HTTPStatus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)


class RestUtil:

    RETRY_COUNT = 6
    BACK_OFF_FACTOR = 0.5
    RETRY_AFTER_STATUS_CODES = (
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT)

    @staticmethod
    def override_response_encoding(r, *args, **kwargs):
        if r.encoding is None:
            r.encoding = 'utf-8'
        return r

    @classmethod
    def request_with_retry(cls, session=None, method_list=[], retry_count=RETRY_COUNT, **kwargs):
        session = session or requests.Session()
        session.hooks['response'].append(RestUtil.override_response_encoding)
        if kwargs.get("verify_ssl") is False:
            session.verify = False

        # some requests might want to pass other status on which they have to be retried.
        # such status should be passed as a tuple
        additional_retry_status_codes = kwargs.get(
            "additional_retry_status_codes", None)
        if additional_retry_status_codes:
            cls.RETRY_AFTER_STATUS_CODES = cls.RETRY_AFTER_STATUS_CODES + \
                additional_retry_status_codes
        # Get connection retry count from arguments. If not provided, default to retry_count.
        connect_retry_count = kwargs.get(
            "connect_retry_count", retry_count)
        #  Construct an iterable set of method to retry.
        method_list = {item for item in method_list +
                       list(Retry.DEFAULT_METHOD_WHITELIST)}

        back_off_factor = cls.BACK_OFF_FACTOR
        if kwargs.get("back_off_factor") is not None:
            back_off_factor = kwargs.get("back_off_factor")
        retry = Retry(
            total=retry_count,
            read=retry_count,
            connect=connect_retry_count,
            method_whitelist=method_list,
            # Time delay between requests is calculated using
            # {backoff factor} * (2 ^ ({number of total retries} - 1)) seconds
            backoff_factor=back_off_factor,
            status_forcelist=cls.RETRY_AFTER_STATUS_CODES,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
