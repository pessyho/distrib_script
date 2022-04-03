#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner


# hack: python2/3 comattibility: remove later
if sys.version_info.major == 3:
    unicode = str

input_data = {
    "bee_id": 2,
    "bee_pwd": "cibeez_123!@#",
    "dist_suffix": ".SP.",
    "plusdays": 0,
    "equalise": 1,
    "merge_areas": False,
    "reset": True,
    "cvrp": True,
    "update_counters": True,
    "email_leftovers": True,
    "email_manifest": True,
    "notify_customers": True,
    "no_lock_drivers": False,
    "areas": None,
    "email_list": None
}


class Component(ApplicationSession):
    @inlineCallbacks
    def onJoin(self, details):
        print("session attached")

        try:
            datas = json.dumps(input_data)
            print("Input data: {}".format(datas))

            res = yield self.call(u'cbz.distro.setup', datas)
            print("Setup:")  # print("Prepare: {}".format(res))

            fd = os.dup(1)
            os.write(fd, (res + "\n").encode())
            os.close(fd)

        except Exception as e:
            print("Error: {}".format(e))

        yield self.leave()

    def onDisconnect(self):
        print("disconnected")
        reactor.stop()


def cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    parser.add_argument('--router', '-R', help='WAMP router to use')
    parser.add_argument('--realm', '-M', help='WAMP realm to use')
    parser.add_argument('--bee_id', '-B', help='Bee ID to login')
    parser.add_argument('--bee_pwd', '-P', help='Bee passwd to login')
    parser.add_argument('--dist_suffix', '-S', help='Dist retailer suffix')
    parser.add_argument('--plusdays', '-D', metavar='DAYS', type=int,
                        help='Add plusdays delay parameter')
    parser.add_argument('--areas', '-A', metavar='AREAS',
                        help='Run for only specified areas (comma separated)')
    parser.add_argument('--merge-areas', dest='merge_areas', action='store_true',
                        help='unite areas into single one')
    parser.add_argument('--no-merge-areas', dest='merge_areas', action='store_false',
                        help='don\'t unite areas into single one (the default)')
    parser.add_argument('--no-equalise', dest='equalise', action='store_false',
                        help='Don\'t equalise vehicles capacities')
    parser.add_argument('--no-notify', '-N', dest='notify_customers',
                        action='store_false',
                        help='Don\'t notify (send SMS) customers')
    parser.add_argument('--no-reset', dest='reset', action='store_false',
                        help='Don\'t reset before CVRP')
    parser.add_argument('--no-cvrp', dest='cvrp', action='store_false',
                        help='Don\'t run CVRP')
    parser.add_argument('--no-update-counters', dest='update_counters',
                        action='store_false',
                        help='Don\'t update counters after CVRP')
    parser.add_argument('--email_list', '-L', metavar='EMAIL ADDRESSES',
                        help='email addreses list')
    parser.add_argument('--no-email-leftovers', dest='email_leftovers',
                        action='store_false', help='Don\'t send leftover email')
    parser.add_argument('--no-lock-drivers', dest='no_lock_drivers',
                        action='store_true',
                        help='Don\'t lock drivers after CVRP')
    parser.add_argument('--no-email-manifest', dest='email_manifest',
                        action='store_false',
                        help='Don\'t send manifest email')
    parser.add_argument('--show-data', default=False, action='store_true',
                        help='Show data would be passed to BE and exit')
    parser.add_argument('--manual-run', action='store_true',
                        help='Flag set if cvrp executed manually by the operator')
    return parser.parse_args()


def set_input_data(vargs):
    for k in input_data.keys():
        if vargs.get(k, None) is not None:
            input_data[k] = vargs[k]


def main():
    args = cmdline()

    set_input_data(vars(args))

    if args.show_data:
        print("Data: {}".format(json.dumps(input_data)))
        return

    router = u"ws://localhost:8080/ws"
    #router = u"wss://be.cibeez.dev.helmes.ee:8443/ws"
    realm = args.realm or u"realm1"
    router = os.environ.get("AUTOBAHN_ROUTER", router)
    realm = os.environ.get("AUTOBAHN_REALM", realm)
    if args.router:
        router = unicode(args.router)
    if args.realm:
        realm = unicode(args.realm)
    runner = ApplicationRunner(router, realm)
    runner.run(Component)


if __name__ == "__main__":
    main()