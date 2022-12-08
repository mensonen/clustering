"""
An example of how to replace the default shared filesystem based storage
backend with a MySQL implementation.

In general, implementing an own backend should provide an instance of a class
that implements `ClusterStorageBackend` and overwrites *at least* the
create_node, get_all_nodes, remove_node, remove_nodes, set_active and
set_primary methods.

This MySQL example is rather rudimentary and does not feature any basic MySQL
connectivity management, or use of any MySQL frameworks. Using this in
production is not advisable.

Following sample MySQL databases/tables should exist for this sample to work:

    ```
    CREATE DATABASE `appdata`
        DEFAULT CHARACTER SET utf8
        COLLATE utf8_unicode_ci;

    CREATE TABLE appdata.app_cluster
    (
        id INT(10) AUTO_INCREMENT PRIMARY KEY,
        cluster_name VARCHAR(100) NOT NULL,
        name VARCHAR(100) NOT NULL,
        ip VARCHAR(15) NOT NULL,
        port INT(4) NOT NULL,
        is_primary TINYINT(1) DEFAULT 0 NOT NULL,
        is_active TINYINT(1) default 0 NOT NULL,
        join_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        version VARCHAR(50) NOT NULL,
        UNIQUE KEY ix_cluster_node (cluster_name, name)
    ) ENGINE=innodb default charset=utf8 collate=utf8_unicode_ci;
    ```

Run from terminal to test:
~# python3 -m examples.alt_backends.mysql_backend

"""
import logging
import MySQLdb
import MySQLdb.cursors
import time

from typing import List

import cluster


class MySQLStorageBackend(cluster.ClusterStorageBackend):
    """A storage backend that uses a MySQL database."""
    def __init__(self, host, user, passwd):
        self.host = host
        self.user = user
        self.passwd = passwd
        self._conn = None

    @property
    def connection(self) -> MySQLdb.Connection:
        if self._conn is None:
            self._conn = MySQLdb.connect(host=self.host, user=self.user,
                                         passwd=self.passwd,
                                         cursorclass=MySQLdb.cursors.DictCursor)
            self._conn.autocommit(True)

        return self._conn

    def create_node(self, node: cluster.ClusterNode):
        cur = self.connection.cursor()
        cur.execute("""INSERT INTO appdata.app_cluster (cluster_name,
                        name, ip, port, is_primary,
                        join_timestamp, version)
                    VALUES (%s, %s, %s, %s, 0, NOW(), %s)
                    ON DUPLICATE KEY UPDATE
                        ip = VALUES(ip),
                        port = VALUES(port),
                        is_primary = VALUES(is_primary),
                        join_timestamp = VALUES(join_timestamp),
                        version = VALUES(version)
                    """, (self.cluster_name, node.name, node.ip, node.port,
                          node.version))
        cur.close()

    def get_all_nodes(self) -> List[cluster.ClusterNode]:
        cur = self.connection.cursor()
        cur.execute("""SELECT * FROM appdata.app_cluster
                    WHERE cluster_name = %s
                    """, (self.cluster_name,))
        nodes = cur.fetchall()
        cur.close()

        return [cluster.ClusterNode(
            n["name"], n["ip"], n["port"], n["join_timestamp"],
            n["version"], n["is_primary"], n["is_active"],
            n["id"]) for n in nodes]

    def remove_node(self, node_name: str):
        cur = self.connection.cursor()
        cur.execute("""DELETE FROM appdata.app_cluster
                    WHERE cluster_name = %s
                        AND name = %s
                    """, (self.cluster_name, node_name))
        cur.close()

    def remove_nodes(self, node_names: List[str]):
        cur = self.connection.cursor()
        for node in node_names:
            cur.execute("""DELETE FROM appdata.app_cluster
                        WHERE cluster_name = %s
                            AND name = %s
                        """, (self.cluster_name, node))
        cur.close()

    def set_active(self, node: cluster.ClusterNode):
        cur = self.connection.cursor()
        cur.execute("""UPDATE appdata.app_cluster 
                    SET is_active = 1
                    WHERE cluster_name = %s
                        AND name = %s
                    """, (self.cluster_name, node.name))
        cur.close()

    def set_primary(self, node: cluster.ClusterNode):
        cur = self.connection.cursor()
        cur.execute("""UPDATE appdata.app_cluster
                    SET is_primary = 0
                    WHERE is_primary = 1
                        AND cluster_name = %s
                    """, (self.cluster_name,))
        cur.execute("""UPDATE appdata.app_cluster
                    SET is_primary = 1
                    WHERE name = %s
                        AND cluster_name = %s
                    """, (node.name, self.cluster_name))
        cur.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(levelname)-7s %(name)-18s %(message)s")

    mysql_backend = MySQLStorageBackend(
        "localhost", "clusterctrl", "secretpass")

    cluster_ctrl = cluster.ClusterControl(
        cluster_name="basic_cluster",  # name of cluster that everyone joins
        host_name="127.0.0.1",  # host address that every node can reach
        node_name="node1",  # our unique name
        hb_port=44330,  # UDP port for our heartbeat
        hb_interval=5,  # interval between heartbeats
        hb_timeout=10,  # seconds until a heartbeat is considered as missed
        check_interval=2,  # how often cluster state should be rechecked
        storage_backend=mysql_backend)

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
