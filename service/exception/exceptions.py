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

from typing import List


class ErrorTarget:
    """Error target model"""

    def __init__(self, error_type, error_name):
        self.type = error_type
        self.name = error_name


class ServiceError(Exception):
    """Base class for exceptions in MRM."""

    def __init__(self, msg, error=None, target: ErrorTarget = None):
        self.message = msg
        self.error = error
        self.target = target


class BadRequestError(ServiceError):
    """Exception raised for invalid input in mrm."""

    def __init__(self, msg, error=None, target: ErrorTarget = None):
        super().__init__(msg, error=error, target=target)


class AuthenticationError(ServiceError):
    """Exception raised when user tried to access with invalid credentials."""

    def __init__(self, msg, error=None, target: ErrorTarget = None):
        super().__init__(msg, error=error, target=target)


class ObjectNotFoundError(ServiceError):
    """Exception raised when the object queried is not found."""

    def __init__(self, msg, error=None, target: ErrorTarget = None):
        super().__init__(msg, error=error, target=target)


class InternalServerError(ServiceError):
    '''Exception raised when unexpected error occur.'''

    def __init__(self, msg, error=None, target: ErrorTarget = None):
        super().__init__(msg, error=error, target=target)


class ServiceErrors(Exception):
    """Exception used to raise multiple errors"""

    def __init__(self, errors: List[ServiceError]):
        self.errors = errors
