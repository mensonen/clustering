"""
A basic example of an application that joins a cluster, becomes primary and
exits if cluster is running a newer version.

This script is identical to node2.py, except that the heartbeat port and node
name have been changed and the "own_version" value is higher than what the
other two examples have.

Run first node from terminal:
~# python3 -m examples.basic.node1

Then run second node from terminal:
~# python3 -m examples.basic.node2

And observe printouts. Kill first node and observe how second node becomes the
primary node.

Run third node from terminal, which has a higher version number:
~# python3 -m examples.basic.node3

And observe how lower version node(s) exit.

"""
import time
import logging
import threading

import cluster

logger = logging.getLogger("main")
exit_event = threading.Event()


def become_primary():
    logger.info("From callback: This node is now primary")


def leave_primary():
    logger.info("From callback: This node is no longer primary")


def forced_exit():
    logger.info("From callback: Cluster has forced this node to exit")
    exit_event.set()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format="%(levelname)-7s %(name)-18s %(message)s")

    shared_backend = cluster.SharedFsStorageBackend("/tmp/clustering")

    cluster_ctrl = cluster.ClusterControl(
        cluster_name="basic_cluster",  # name of cluster that everyone joins
        host_name="127.0.0.1",  # host address that every node can reach
        node_name="node3",  # our unique name
        hb_port=44332,  # UDP port for our heartbeat
        hb_interval=5,  # interval between heartbeats
        hb_timeout=10,  # seconds until a heartbeat is considered as missed
        check_interval=2,  # how often cluster state should be rechecked
        start_primary_callback=become_primary,
        stop_primary_callback=leave_primary,
        exit_callback=forced_exit,
        hb_missed_count=2,  # remove nodes after 2 misses (a bit low for production)
        own_version="1.1",  # everyone with less than 1.0 will exit
        storage_backend=shared_backend)

    try:
        logger.info("Joining cluster")
        cluster.join_cluster(cluster_ctrl)
        logger.info("Starting cluster control")
        cluster_ctrl.start()

        # busy loop
        while True:
            time.sleep(1)

            if exit_event.is_set():
                raise SystemExit()

    except (KeyboardInterrupt, SystemExit):
        logger.info("Gracefully exiting cluster")
        # the order is important: if cluster is left before controller stops,
        # the controller will just auto-rejoin on its own
        cluster_ctrl.stop()
        cluster_ctrl.join(10)
        cluster.leave_cluster(cluster_ctrl)

    print("Exited")
