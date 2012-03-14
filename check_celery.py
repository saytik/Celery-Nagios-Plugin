#! /usr/bin/python

"""
check_celery.py
~~~~~~~~~

This is a monitoring plugin for Nagios NRPE. If required, ensure a copy of this is 
placed in the following directory on the host machine: /usr/local/nagios/libexec/
"""
import sys
import requests
import simplejson as json
from NagAconda import Plugin

check_api = Plugin("Used to determine the status of a Celery worker.", "1.0")

check_api.add_option("p", "port", "Port of the Celery host machine serving the Celerymon API. (default: 8989)", default=8989)
check_api.add_option("h", "host", "Host of the Celery worker instance. (default: http://localhost)", default="http://localhost")
check_api.add_option("a", "action", "The status check to perform. (nodeup, health)", default="health")
check_api.add_option("n", "node", "Check if a specified node is up. Used with `nodeup` action. (default: celery.ubuntu)", default="celery.ubuntu")
check_api.add_option("l", "limit", "Number of tasks in the past to check. (default: 100)", default=100)

check_api.enable_status("warning")
check_api.enable_status("critical")

check_api.start()

if check_api.options.action not in ("nodeup", "health"):
    check_api.unknown_error("unknown action specified %s." % check_api.options.action)

response = requests.get("%s:%d/api/worker/" % (check_api.options.host, int(check_api.options.port)))

try:
    response.raise_for_status()
except Exception as e:
    print "Status Critical, celerymon API not reachable"
    sys.exit(2)

try:
    content = json.loads(response.content)
except Exception as e:
    check_api.unknown_error("%s health check response was malformed: %s" % (check_api.options.action, e))

if len(content) == 0:
    print "Status Ok, nothing in celery queue at the moment"
    sys.exit(0)

if check_api.options.action == "nodeup":
    response = requests.get("%s:%d/api/worker/%s" % (check_api.options.host, int(check_api.options.port), check_api.options.node))

    try:
        response.raise_for_status()
    except Exception as e:
        print "Status Critical, %s node not found" % check_api.options.node
        sys.exit(2)

    try:
        content = json.loads(response.content)
    except Exception as e:
        check_api.unknown_error("%s health check response was malformed: %s" % (check_api.options.action, e))
else:
    response = requests.get("%s:%d/api/tasks/?limit=%d" % (check_api.options.host, int(check_api.options.port), check_api.options.limit))

    try:
        response.raise_for_status()
    except Exception as e:
        print "Status Critical, task list for node %s cannot be retrieved" % check_api.options.node
        sys.exit(2)

    try:
        content = json.loads(response.content)
    except Exception as e:
        check_api.unknown_error("%s health check response was malformed: %s" % (check_api.options.action, e))

    failed = []
    for task in content:
        if task[1]["failed"]:
            failed.append(task[0])

    if failed:
        print "Status Warning, the last %d tasks for node %s contain failures: %s" % (check_api.options.limit, check_api.options.node, failed)
        sys.exit(1)

check_api.set_status_message("Celery health check successful")

check_api.finish()
