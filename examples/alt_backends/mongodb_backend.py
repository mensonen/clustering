"""
An example of how to replace the default shared filesystem based storage
backend with a MongoDB implementation.

In general, implementing an own backend should provide an instance of a class
that implements `ClusterStorageBackend` and overwrites *at least* the
create_node, get_all_nodes, remove_node, remove_nodes, set_active and
set_primary methods.

This MongoDB example is rather rudimentary and does not feature any basic
connectivity management, or even use authentication for the connection, please
do not use in production.

This sample assumes that a MongoDB instance is running on localhost.

Run from terminal to test:
~# python3 -m examples.alt_backends.mongodb_backend

"""
import json
import logging
import pymongo
import time

from typing import List

import cluster


class MongoDbStorageBackend(cluster.ClusterStorageBackend):
    """A storage backend that uses a MongoDB database."""
    def __init__(self, connection_uri):
        self.db = pymongo.MongoClient(connection_uri, connect=False)

    def create_node(self, node: cluster.ClusterNode):
        # quickest way to get a "clean" version of the node, without any
        # internal attributes that should not be stored in database
        document = json.loads(node.to_json())
        document["cluster_name"] = self.cluster_name

        self.db.app_cluster.cluster_nodes.update_one(
            {"cluster_name": self.cluster_name, "name": node.name},
            {"$set": document},
            upsert=True)

    def get_all_nodes(self) -> List[cluster.ClusterNode]:
        nodes = self.db.app_cluster.cluster_nodes.find(
            {"cluster_name": self.cluster_name})

        return [cluster.ClusterNode(
            n["name"], n["ip"], n["port"], n["join_timestamp"],
            n["version"], n["is_primary"], n["is_active"],
            n["id"]) for n in nodes]

    def remove_node(self, node_name: str):
        self.db.app_cluster.cluster_nodes.delete_one(
            {"cluster_name": self.cluster_name, "name": node_name})

    def remove_nodes(self, node_names: List[str]):
        for node in node_names:
            self.db.app_cluster.cluster_nodes.delete_one(
                {"cluster_name": self.cluster_name, "name": node})

    def set_active(self, node: cluster.ClusterNode):
        self.db.app_cluster.cluster_nodes.update_one(
            {"cluster_name": self.cluster_name, "name": node.name},
            {"$set": {"is_active": True}})

    def set_primary(self, node: cluster.ClusterNode):
        self.db.app_cluster.cluster_nodes.update_one(
            {"cluster_name": self.cluster_name, "is_primary": True},
            {"$set": {"is_primary": False}})
        self.db.app_cluster.cluster_nodes.update_one(
            {"cluster_name": self.cluster_name, "name": node.name},
            {"$set": {"is_primary": True}})


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)-7s %(name)-18s %(message)s")

    # no authentication obviously not good for production
    mongodb_backend = MongoDbStorageBackend("mongodb://localhost:27017")

    cluster_ctrl = cluster.ClusterControl(
        cluster_name="basic_cluster",  # name of cluster that everyone joins
        host_name="127.0.0.1",  # host address that every node can reach
        node_name="node1",  # our unique name
        hb_port=44330,  # UDP port for our heartbeat
        hb_interval=5,  # interval between heartbeats
        hb_timeout=10,  # seconds until a heartbeat is considered as missed
        check_interval=2,  # how often cluster state should be rechecked
        storage_backend=mongodb_backend)

    try:
        cluster.join_cluster(cluster_ctrl)
        cluster_ctrl.start()

        # busy loop
        while True:
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        # the order is important: if cluster is left before controller stops,
        # the controller will just auto-rejoin on its own
        cluster_ctrl.stop()
        cluster_ctrl.join(10)
        cluster.leave_cluster(cluster_ctrl)
