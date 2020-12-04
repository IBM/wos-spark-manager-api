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

from flask import Flask, request

from service.config import DevelopmentConfig, ProductionConfig
from service.resources.api_blueprint import api_blueprint
from service.security.auth import Auth
from service.utils.environment import Environment
from service.utils.kerberos_util import KerberosUtil
from service.utils.sw_logger import SwLogger
from service.utils.sw_session import SwSession
from service.utils.sw_session_manager import SwSessionManager

logger = SwLogger(__name__)


def create_app(configuration=None):
    app = Flask(__name__)

    if configuration:
        app.config.from_object(configuration)

    # Allow trailing slashes for all rest endpoints
    app.url_map.strict_slashes = False
    app.register_blueprint(api_blueprint)

    @app.before_request
    def before_request():
        # skip authentication and authorization for heartbeat and swagger APIs
        if request.endpoint not in ("api.doc", "restplus_doc.static", "api.specs"):

            auth = Auth(request.headers)
            username, user_principal = auth.authenticate()

            # create the session object
            session = SwSession()
            session.set_username(username)
            if user_principal is not None:
                session.set_user_principal(user_principal)
            # set the session object into session manager so that it can accessed down stream
            SwSessionManager().set_session(session)

            # log the entry
            log_entry()

    @app.after_request
    def after_request(response):
        if request.endpoint not in ("api.doc", "restplus_doc.static", "api.specs"):
            log_exit()
            # using 1 year max-age
            response.headers["Strict-Transport-Security"] = "max-age=31536000 ; includeSubDomains"
            # Define loading policy for all resources type in case of a resource type
            # dedicated directive is not defined (fallback)
            response.headers["X-Content-Type-Options"] = "nosniff"
            # The cache should not store anything about the client request or server response.
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            # For older browsers that do not support Cache-Control.
            response.headers["Pragma"] = "no-cache"
            # Block pages from loading when they detect reflected XSS attacks:
            response.headers["X-XSS-Protection"] = "1; mode=block"
            # Define loading policy for all resources type in case of a resource type
            # dedicated directive is not defined (fallback)
            response.headers["Content-Security-Policy"] = "default-src 'none'"
        return response

    def log_entry():
        operation = request.method + "-" + str(request.path)
        logger.log_info("Starting sw service request " + operation)

    def log_exit():
        operation = request.method + "-" + str(request.path)
        logger.log_info("Completed sw service request " + operation)

    return app


if __name__ == "__main__":
    print("Starting mrm app in development mode...")
    app = create_app(DevelopmentConfig())
    app.run(port=5000)
else:
    app = create_app(ProductionConfig())
