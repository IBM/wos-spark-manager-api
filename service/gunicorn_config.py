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

import multiprocessing
import socket


from service.utils.sw_logger import SwLogger

logger = SwLogger(__name__)

cpu_count = multiprocessing.cpu_count()

#
# Server mechanics
#
localhost_name = socket.gethostname()
bind = localhost_name + ":9443"
#

# Logging
#
errorlog = "-"
loglevel = "info"
accesslog = None
access_log_format = "%(t)s '%(r)s' %(s)s %(b)s '%(f)s' '%(a)s'"

#
# Worker processes
#
#workers = (2 * cpu_count) + 1
workers = 5
worker_class = "gthread"
threads = 500
timeout = 1800  # 30 minutes
keepalive = 2

#
# Server hooks
#


def post_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)


def pre_fork(server, worker):
    pass


def pre_exec(server):
    server.log.info("Forked child, re-executing.")


def when_ready(server):
    server.log.info("Server is ready. Spawning workers")


def worker_int(worker):
    worker.log.info("worker received INT or QUIT signal")

    # get traceback info
    import threading
    import sys
    import traceback
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for thread_id, stack in sys._current_frames().items():
        code.append("\n# Thread: %s(%d)" %
                    (id2name.get(thread_id, ""), thread_id))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append("File: '%s', line %d, in %s" %
                        (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    worker.log.debug("\n".join(code))


def worker_abort(worker):
    logger.log_info("Worker (pid: {0}) aborting.".format(worker.pid))
    worker.log.info("worker received SIGABRT signal")


def worker_exit(server, exit):
    logger.log_info("Worker exiting.")
    logger.log_info("Worker exited.")
