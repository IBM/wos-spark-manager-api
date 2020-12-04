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

class Config(object):
    """Parent configuration class."""
    DEBUG = False
    TESTING = False
    # Swagger config parameters
    SWAGGER_UI_DOC_EXPANSION = "list"
    RESTPLUS_MASK_SWAGGER = False
    # Exclude default message attribute when returning error json
    ERROR_INCLUDE_MESSAGE = False
    ERROR_404_HELP = False


class DevelopmentConfig(Config):
    """Configurations for Development."""
    DEBUG = True
    JSON_LOGGING_ENABLED = False


class TestingConfig(Config):
    """Configurations for Testing."""
    TESTING = True
    DEBUG = True


class ProductionConfig(Config):
    """Configurations for Production."""
    DEBUG = False
    TESTING = False
