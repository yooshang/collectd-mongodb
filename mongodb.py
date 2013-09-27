#
# Plugin to collectd statistics from MongoDB
#

import collectd
import types
from pymongo import Connection
from distutils.version import StrictVersion as V
from copy import deepcopy


# maps that duplicate the key/nested-key structure for returned data from mongodb commands but mapping
# to values that are a tuple containing the value's collectd type (types.db)
# and optionally the collectd value name (defaults to nested key path to the leaf value with key strings joined with ".")
# NOTE: some metrics are more complicated and thus are handled specially (ex. connPoolStats hosts data)
CONNECTION_POOL_STATUS_METRICS = {
    # db.runCommand({connPoolStats:1})
    # hosts and replicaSets handled specially
    "createdByType": {
        "master": "total_connections",
        "set": "total_connections",
        "sync": "total_connections"
    },
    "totalAvailable": "gauge",
    "totalCreated": "total_connections",
    "numDBClientConnection": "gauge",
    "numAScopedConnection": "gauge"
}

DBSTATS_METRICS = {
    # db.runCommand({dbStats:1})
    "collections": "gauge",
    "objects" : "gauge",
    "avgObjSize" : "bytes",
    "dataSize" : "bytes",
    "storageSize" : "bytes",
    "numExtents" : "gauge",
    "indexes" : "gauge",
    "indexSize" : "bytes",
    "fileSize" : "bytes",
    "nsSizeMB": "gauge"
}

SERVER_STATUS_METRICS = {
    # db.runCommand({serverStatus:1})
    "backgroundFlushing": {
        "last_ms": "gauge" 
    },
    "connections": {
        "current": "gauge",
        "available": "gauge"
    },
    "cursors": {
        "totalOpen": "gauge",
        "timedout": "derive"
    },
    "globalLock": {
        "currentQueue" : {
            "total" : "gauge",
            "readers" : "gauge",
            "writers" : "gauge"
        },
        "activeClients" : {
            "total" : "gauge",
            "readers" : "gauge",
            "writers" : "gauge"
        }
    },
    "indexCounters" : {
        "accesses" : "derive",
        "hits" : "derive",
        "misses" : "derive"
    },
    "locks": {
        "." : {
            "timeLockedMicros" : {
                "R" : "derive",
                "W" : "derive"
            },
            "timeAcquiringMicros" : {
                "R" : "derive",
                "W" : "derive"
            }
        }
        # special handling for other configured DBs
    },
    "opcounters": {
        "insert" : "derive",
        "query" : "derive",
        "update" : "derive",
        "delete" : "derive",
        "getmore" : "derive",
        "command" : "derive"
    },
    "recordStats": {
        "accessesNotInMemory" : "derive",
        "pageFaultExceptionsThrown" : "derive"
        # do we want db specificity?
    },
    "mem": {
        "bits" : "gauge",
        "resident" : "gauge",
        "virtual" : "gauge",
        "mapped" : "gauge",
        "mappedWithJournal" : "gauge"
    },
    "metrics" : {
        "document" : {
            "deleted" : "derive",
            "inserted" : "derive",
            "returned" : "derive",
            "updated" : "derive"
        }
    }
}


class MongoDB(object):


    def __init__(self):
        self.plugin_name = "mongo"
        self.mongo_host = "127.0.0.1"
        self.mongo_port = 27017
        self.mongo_dbs = ["admin", ]
        self.mongo_user = None
        self.mongo_password = None

        self.includeConnPoolMetrics = None
        self.includeServerStatsMetrics = None
        self.includeDbstatsMetrics = None


    def connect(self):
        global SERVER_STATUS_METRICS
        self.mongo_client = Connection(host=self.mongo_host, port=self.mongo_port, slave_okay=True)
        if not self.mongo_client.alive():
            collectd.error("mongodb plugin failed to connect to %s:%d" % (self.mongo_host, self.mongo_port))

        server_status = self.mongo_client[self.mongo_dbs[0]].command("serverStatus")
        version = server_status["version"]
        at_least_2_4 = V(version) >= V("2.4.0")
        if not at_least_2_4:
            indexCountersMap = SERVER_STATUS_METRICS.pop("indexCounters")
            SERVER_STATUS_METRICS["indexCounters"] = {"btree": indexCountersMap}


    def disconnect(self):
        if self.mongo_client and self.mongo_client.alive():
            self.mongo_client.disconnect()


    def config(self, obj):
        for node in obj.children:
            if node.key == "Port":
                self.mongo_port = int(node.values[0])
            elif node.key == "Host":
                self.mongo_host = node.values[0]
            elif node.key == "User":
                self.mongo_user = node.values[0]
            elif node.key == "Password":
                self.mongo_password = node.values[0]
            elif node.key == "Databases":
                self.mongo_dbs = node.values
            elif node.key == "ConnectionPoolStatus":
                self.includeConnPoolMetrics = node.values
            elif node.key == "ServerStats":
                self.includeServerStatsMetrics = node.values
            elif node.key == "DBStats":
                self.includeDbstatsMetrics = node.values
            else:
                collectd.warning("mongodb plugin: Unkown configuration key %s" % node.key)


    def submit(self, instance, type, value, db=None):
        # actually a recursive submit call to dive deeper into nested dict data
        # since the leaf value in the nested dicts is the type, we check on the type type :-)
        if db:
            plugin_instance = "%s-%d" % (self.mongo_port, db)
        else:
            plugin_instance = str(self.mongo_port)
        v = collectd.Values()
        v.plugin = self.plugin_name
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        v.dispatch()


    def recursive_submit(self, type_tree, data_tree, instance_name=None, db=None):
        # if we are still in the middle of the type and data tree
        if isinstance(type_tree, types.DictType) and isinstance(data_tree, types.DictType):
            for type_name, type_value in type_tree.iteritems():
                next_instance_name = None
                if instance_name:
                    next_instance_name = instance_name + "." + type_name
                else:
                    next_instance_name = type_name
                if data_tree.has_key(type_name):
                    self.recursive_submit(type_value, data_tree[type_name], next_instance_name)
                else:
                    # may want to log this but some mongodb setups may not have anything to report
                    pass
        elif isinstance(type_tree, types.DictType) or isinstance(data_tree, types.DictType):
            print("type tree and data tree structure differ for data instance: " + instance_name)
        else:
            self.submit(instance_name, type_tree, data_tree, db)


    def publish_connection_pool_metrics(self):
        # connPoolStats produces the same results regardless of db used
        db = self.mongo_client[self.mongo_dbs[0]]
        if self.mongo_user and self.mongo_password:
            db.authenticate(self.mongo_user, self.mongo_password)

        conn_pool_stats = db.command("connPoolStats")
        metrics_to_collect = {}
        if self.includeConnPoolMetrics:
            for root_metric_key in self.includeConnPoolMetrics.iterkeys():
                if conn_pool_stats.has_key(root_metric_key):
                    metrics_to_collect[root_metric_key] = deepcopy(CONNECTION_POOL_STATUS_METRICS[root_metric_key])
        else:
            metrics_to_collect = CONNECTION_POOL_STATUS_METRICS

        self.recursive_submit(metrics_to_collect, conn_pool_stats)


    def publish_dbstats(self):
        for db_name in self.mongo_dbs:
            db = self.mongo_client[db_name]
            if self.mongo_user and self.mongo_password:
                db.authenticate(self.mongo_user, self.mongo_password)

            dbstats = db.command("dbStats")
            metrics_to_collect = {}
            if self.includeDbstatsMetrics:
                for root_metric_key in self.includeDbstatsMetrics.iterkeys():
                    if dbstats.has_key(root_metric_key):
                        metrics_to_collect[root_metric_key] = deepcopy(DBSTATS_METRICS[root_metric_key])
            else:
                metrics_to_collect = DBSTATS_METRICS

            self.recursive_submit(metrics_to_collect, dbstats, db=db_name)


    def publish_server_status(self):
        # serverStatus produces the same results regardless of db used
        db = self.mongo_client[self.mongo_dbs[0]]
        if self.mongo_user and self.mongo_password:
            db.authenticate(self.mongo_user, self.mongo_password)

        server_status = db.command("serverStatus")
        metrics_to_collect = {}
        if self.includeServerStatsMetrics:
            for root_metric_key in self.includeServerStatsMetrics.iterkeys():
                if server_status.has_key(root_metric_key):
                    metrics_to_collect[root_metric_key] = deepcopy(SERVER_STATUS_METRICS[root_metric_key])
        else:
            metrics_to_collect = deepcopy(SERVER_STATUS_METRICS)
        # rename "." lock to be "GLOBAL"
        if metrics_to_collect["locks"].has_key("."):
            print(SERVER_STATUS_METRICS["locks"])
            global_lock_data = metrics_to_collect["locks"].pop(".")
            metrics_to_collect["locks"]["GLOBAL"] = global_lock_data

            print(SERVER_STATUS_METRICS["locks"])
        for db_name in self.mongo_dbs:
            metrics_to_collect["locks"][db_name] = deepcopy(SERVER_STATUS_METRICS["locks"]["."])

        self.recursive_submit(metrics_to_collect, server_status)


    def publish_data(self):
        self.publish_server_status()
        self.publish_connection_pool_metrics()
        self.publish_dbstats()


mongodb = MongoDB()
collectd.register_read(mongodb.publish_data)
collectd.register_config(mongodb.config)
collectd.register_init(mongodb.connect)
collectd.register_shutdown(mongodb.disconnect)
