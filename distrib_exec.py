#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------------------------------------------------
#   distrib_script
#                   controls manual and automatic (crontab) exec of CVRP
#                   to avoid multiple exec the same day: manual and auto
#
#   version     1.2     - using db flg 'MB_distrib_exec_manual_run'
#               1.3     - use manual exec date in   'MB_distrib_exec_manual_run'
#               1.4     - added check for thurs man run and --no-rest option if manual run
#               1.5     - cvrp only for a predefined chunk of data, defined in MB_distrib_chunk_sie
#
# --------------------------------------------------------------------------------------------------------------------------


VERSION = '1.5'


# constants for order flag settings
ORDER_FLAG_PREDEFINED_TW = 0x4
ORDER_FLAG_DELIVERY_NOTIFIED = 0x1
ORDER_FLAG_FIXED = 0x8

import os
import os.path
import sys
from sys import platform as _platform
import json
import argparse
import logging
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
import mysql.connector
from mysql.connector import errorcode
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import datetime as dt
import pandas as pd


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

def init_db():
    # db config , iniot db connector and sshtunnel
    #wd = os.environ.get('DSS_WD', '.')
    #cwd = os.getcwd()
    if _platform != 'darwin':
        option_files = "/home/pessyhollander/cibeez/scripts/config/dss-mysql.cnf"  #os.path.join(cwd, "config/dss-mysql.cnf")
    else:
        option_files = "./config/dss-mysql.cnf"
    if not os.path.isfile(option_files):
        logging.debug('ERR missing dss-mysql.cnf configuraion file ')
        return None, None, None

    config = {
        'option_files': option_files,  # "'dss-mysql.cnf',
        'option_groups': ['Client', 'DSS']
    }

    # -------------------------------------------------------------
    # this peace of code is used only when testing from a client
    # -------------------------------------------------------------
    logging.debug("Executed from " + _platform + " machine ")
    # when using from client, set to your own config
    ssh_tunnel_host = None
    try:
        ssh_tunnel_host = os.environ['DSS_TUNNEL_HOST']
        ssh_private_key = '/Users/pessyhollander/.ssh/id_rsa'
        ssh_username = 'pessyhollander'
    except:
        pass

    server = None

    if ssh_tunnel_host:
        import sshtunnel
        logging.debug("Initiating sshtunnel from a MAC OS machine ")
        print("Initiating sshtunnel from a MAC OS machine ")
        try:
            server = sshtunnel.SSHTunnelForwarder(
                (ssh_tunnel_host, 22),
                ssh_private_key=ssh_private_key,
                ssh_username=ssh_username,
                remote_bind_address=('127.0.0.1', 3306),
            )
            server.start()
            connected_port = server.local_bind_port
            logging.debug("connected to port: {0}".format(connected_port))
        except Exception as e:
            logging.debug('sshtunnel forwarding failed: {0}'.format(e))
            return None, None, None

        config['host'] = '127.0.0.1'
        config['port'] = connected_port
    # ------------------------------------------------------------

    try:
        db_connector = mysql.connector.connect(**config)
        logging.debug("Connected to db, db connection: {0}".format(db_connector))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.debug("ERR: (mysql.connector) Something is wrong with your user name or password. err: ֻֻ{0}".format(
                err.errno))
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.debug("ERR: (mysql.connector) Database does not exist. err: ֻֻ{0}".format(err.errno), 'debug')
        else:
            logging.debug("ERR: (mysql.connector) {0}".format(err))
        return None, None, None

    return db_connector, ssh_tunnel_host, server


class Component(ApplicationSession):
    @inlineCallbacks
    def onJoin(self, details):
        print("session attached")

        try:
            datas = json.dumps(input_data)
            #print("Input data: {}".format(datas))
            logging.debug("Input data: {}".format(datas))

            res = yield self.call(u'cbz.distro.setup', datas)
            #print("Setup:")  # print("Prepare: {}".format(res))
            logging.debug("Setup:")

            fd = os.dup(1)
            os.write(fd, (res + "\n").encode())
            os.close(fd)

        except Exception as e:
            #print("Error: {}".format(e))
            logging.debug("Error: {}".format(e))

        yield self.leave()

    def onDisconnect(self):
        #print("disconnected")
        logging.debug("disconnected")
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
                        action='store_false', help='Don\'t notify (send SMS) customers')
    parser.add_argument('--no-reset', dest='reset', action='store_false', help='Don\'t reset before CVRP')
    parser.add_argument('--no-cvrp', dest='cvrp', action='store_false', help='Don\'t run CVRP')
    parser.add_argument('--no-update-counters', dest='update_counters',
                        action='store_false', help='Don\'t update counters after CVRP')
    parser.add_argument('--email_list', '-L', metavar='EMAIL ADDRESSES',
                        help='email addreses list')
    parser.add_argument('--no-email-leftovers', dest='email_leftovers',
                        action='store_false', help='Don\'t send leftover email')
    parser.add_argument('--no-lock-drivers', dest='no_lock_drivers',
                        action='store_true', help='Don\'t lock drivers after CVRP')
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



# read the data and prepare for manifest with limited
def prepare_manifest_chunk(o_type_d, db_connector):

    manifest_df = pd.DataFrame()
    logging.debug("reading chunk size from config ....")
    mycursor = db_connector.cursor()
    sql = "SELECT value FROM configs WHERE field = 'MB_distrib_chunk_size' ;"
    mycursor.execute(sql)
    # db_connector.commit()
    myresult = mycursor.fetchall()
    chunk_size = int(myresult[0][0])
    msg = "chunk size defined in 'MB_distrib_chunk_size': {}".format(chunk_size)
    print(msg)
    logging.debug(msg)


    # sql = "UPDATE orders SET o_order_state = 1, o_vehicle_id = %s, o_seq = %s, o_payment_id = %s WHERE id = %s"

    #db_connector, ssh_tunnel_host, server = init_db()

    try:
        sql = "SELECT " \
                "id, o_payment_id, o_req_uid, o_external_id, o_order_state, o_pickup_time_planned, updated_at " \
                "FROM orders " \
                "WHERE o_order_state IN (2) " \
                "AND o_type_d LIKE 'DIST" + str(o_type_d) + "%' " \
                "AND DATE(orders.updated_at) BETWEEN DATE(SUBDATE(NOW(),2))  AND DATE(CURDATE()) ORDER by orders.o_pickup_time_planned ASC ;"
        logging.debug("read_sql: {}".format(sql))
        manifest_df = pd.read_sql(sql, db_connector)
    except Exception as err:
        logging.error("prepare_manifest_chunk(), read_sql err: {0}".format(err))
        return None

    if manifest_df.empty:
        msg = "prepare_manifest_chunk(), o_type_d: {0} input is empty for date: {1}".format(o_type_d, dt.datetime.now().strftime("%Y-%m-%d"))
        logging.debug(msg)
        return None
    else:
        msg = "prepare_manifest_chunk() OK, returned {0} records".format(manifest_df.shape[0])
        print(msg)
        logging.debug(msg)


    cvrp_records = manifest_df.shape[0]
    if chunk_size > cvrp_records:
        msg = "chunk size ({0}) > data size ({1}), euqlizing...".format(chunk_size,cvrp_records)
        print(msg)
        logging.debug(msg)
        chunk_size = cvrp_records


    # now, reset all orders that shouldnt go into the manifest with o_payment_id = 0x0 and the rest to 0x8
    #o_req_uid = manifest_df.loc[0:0, 'o_req_uid'].tolist()[0]
    manifest_df.loc[0:chunk_size, 'o_payment_id'] = 0x0
    manifest_df.loc[chunk_size:, 'o_payment_id'] = ORDER_FLAG_FIXED
    msg = "Updating records: 0-{0} with 0x0 and {1}-{2} with  0x8".format(chunk_size,chunk_size, cvrp_records)
    print(msg)
    #ids_in_chunck = ','.join([str(e) for e in manifest_df.loc[chunk_size:, 'id'].tolist()])
    #ids_out_chunck = ','.join([str(e) for e in manifest_df.loc[0:chunk_size, 'id'].tolist()])
    #sql = "UPDATE orders SET o_payment_id = %s WHERE id IN (%s) AND o_req_uid = %s ; "
    #data = (o_payment_id, id, o_req_uid)

    for _, row in manifest_df.iterrows():
        o_payment_id = int(row.o_payment_id)
        id = int(row.id)
        o_req_uid = int(row.o_req_uid)
        sql = "UPDATE orders SET o_payment_id = %s WHERE id = %s AND o_req_uid = %s ; "
        data = (o_payment_id, id, o_req_uid)
        try:
            mycursor.execute(sql, data)
        except Exception as err:
            msg = "prepare_manifest_chunk()/UPDATE  Exception: {0}. sql: {1}".format(err, sql)
            print(msg)
            logging.error(msg)
            db_connector.close()
            return False
    db_connector.commit()  # commit all updates


    return True

def main():
    #cwd = os.getcwd()
    if _platform != 'darwin':
        logfile = "/home/pessyhollander/cibeez/scripts/log/distrib_exec.log"
    else:
        logfile = "./log/distrib_exec.log"
    #os.chmod(logfile, 0o666) # rw by all
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s :: %(message)s'
    logging.basicConfig(filename=logfile, level=logging.DEBUG,
                        format=FORMAT, datefmt='%Y%m%d %H:%M:%S')

    logging.debug("{0} version: {1}".format(os.path.basename(sys.argv[0]),VERSION))
    # init db
    db_connector, ssh_tunnel_host, server = init_db()
    if db_connector is None:
        logging.debug("init_db() failed, terminating ....")
        return

    # read cmdline args
    args = cmdline()

    set_input_data(vars(args))

    if args.show_data:
        print("Data: {}".format(json.dumps(input_data)))
        return

    logging.debug("input data: {}".format((json.dumps(input_data))))
    today = dt.datetime.now().strftime("%Y-%m-%d")


    if args.manual_run: # set flag and continue execution

        # manual run, prepare chunk
        pre_process_ok = prepare_manifest_chunk(args.dist_suffix, db_connector)
        if not pre_process_ok:
            logging.debug("input prepare_manifest_chunk failed... terminating ")
            return


        input_data["reset"] = False # manual run -> no reset
        logging.debug("manual run...update 'MB_distrib_exec_manual_run'  flag file and disable 'reset' ....")
        mycursor = db_connector.cursor()
        sql = "UPDATE configs SET value = '{0}'  WHERE field = 'MB_distrib_exec_manual_run'  ;".format(today)
        mycursor.execute(sql)
        db_connector.commit()
        db_connector.close()
        if ssh_tunnel_host: server.stop()

    else: # run from crontab, check if manual run executed before,
        logging.debug("executed from crontab... checking if manual run preceeded ....")
        mycursor = db_connector.cursor()
        sql = "SELECT value FROM configs WHERE field = 'MB_distrib_exec_manual_run' ;"
        mycursor.execute(sql)
        #db_connector.commit()
        myresult = mycursor.fetchall()
        flag_date = str(myresult[0][0])
        # check if a manual run was exec on thursday and now its friday
        if flag_date != '':
            is_thur_friday = (dt.datetime.strptime(flag_date, "%Y-%m-%d").weekday() == 3) and (dt.datetime.now().weekday() == 4)
        else:
            is_thur_friday = False
        flag = (today == flag_date) or is_thur_friday  # set flag if manual run already executed
        logging.debug("'MB_distrib_exec_manual_run flag' : {0} ".format(flag))
        if flag:  # manual run preceeded, clear flag and exit
            logging.debug("manual run preceeded, doing nothing...")
            if is_thur_friday:
                logging.debug("Thursday manual run, clearing 'MB_distrib_exec_manual_run'")
                mycursor = db_connector.cursor()
                sql = "UPDATE configs SET value = ''  WHERE field = 'MB_distrib_exec_manual_run'  ;"
                mycursor.execute(sql)
                db_connector.commit()
            db_connector.close()
            if ssh_tunnel_host: server.stop()
            return
        else:
            logging.debug("no manual run preceeded, runninig crontab...")

            # prepare chunk before execution
            pre_process_ok = prepare_manifest_chunk(args.dist_suffix, db_connector)
            if not pre_process_ok:
                logging.debug("input prepare_manifest_chunk failed... terminating ")
                return


            db_connector.close()
            if ssh_tunnel_host: server.stop()


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