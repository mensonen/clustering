"""
A generic clustering control mechanism

This module contains classes and methods to create clustered instances of any
running software, with member status observing, start/stop callbacks and UDP
heartbeats.

A very quick start guide:

 1. Create an instance of `ClusterControl`
 2. Call `join_cluster`
 3. Call `ClusterControl.start`
 4. When done, call `ClusterControl.stop`, followed by `ClusterControl.join`
 5. Call `leave_cluster`

"""
from __future__ import annotations

import datetime
import json
import logging
import pathlib
import random
import select
import socket
import threading
import time

from distutils.version import LooseVersion
from typing import List, Callable, Optional


class StoppableThread(threading.Thread):
    """A thread that can be stopped gracefully

    Thread class with a stop() method. The thread itself has to check
    regularly for the `self.is_stopped` condition within the `run()` method and
    break its main loop gracefully once the condition is set.
    """
    def __init__(self):
        super().__init__()
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    @property
    def is_stopped(self):
        return self._stop_event.isSet()


class ClusterStorageBackend:
    cluster_name: str

    def create_node(self, node: ClusterNode):
        """Add a new node into storage."""
        raise NotImplementedError()

    def get_all_nodes(self) -> List[ClusterNode]:
        """Retrieve all known cluster nodes from storage."""
        raise NotImplementedError()

    def remove_node(self, node_name: str):
        """Entirely deletes node data from storage.

        Args:
            node_name: Name of node to remove
        """
        raise NotImplementedError()

    def remove_nodes(self, node_names: List[str]):
        """Entirely deletes node data from storage.

        Args:
            node_names: A list of names of nodes to remove.
        """
        raise NotImplementedError()

    def set_cluster_name(self, cluster_name: str):
        """Set the name of the cluster to store."""
        self.cluster_name = cluster_name

    def set_active(self, node: ClusterNode):
        """Flag a node as active."""
        raise NotImplementedError()

    def set_primary(self, node: ClusterNode):
        """Set a node as primary.

        Also unsets any previously set primary node.
        """
        raise NotImplementedError()


class SharedFsStorageBackend(ClusterStorageBackend):
    """A storage backend that uses a shared file system."""

    def __init__(self, storage_root: str):
        self.storage_root = storage_root
        self.storage_path: Optional[pathlib.Path] = None

    def create_node(self, node: ClusterNode):
        if not node.id:
            node.id = random.getrandbits(64)
        self.write_node(node)

    def get_all_nodes(self) -> List[ClusterNode]:
        nodes = []
        for item in self.storage_path.iterdir():
            with item.open("r") as f:
                nodes.append(ClusterNode.from_json(f.read()))

        return nodes

    def remove_node(self, node_name: str):
        node_path = pathlib.Path(self.storage_path, node_name)
        try:
            node_path.unlink()
        except FileNotFoundError:
            pass

    def remove_nodes(self, node_names: List[str]):
        for node_name in node_names:
            node_path = pathlib.Path(self.storage_path, node_name)
            try:
                node_path.unlink()
            except FileNotFoundError:
                pass

    def set_cluster_name(self, cluster_name: str):
        self.storage_path = pathlib.Path(self.storage_root, cluster_name)
        if not self.storage_path.is_dir():
            self.storage_path.mkdir(parents=True, exist_ok=True)

    def set_active(self, node: ClusterNode):
        node.is_active = True
        self.write_node(node)

    def set_primary(self, node: ClusterNode):
        for stored_node in self.get_all_nodes():
            if stored_node.is_primary:
                stored_node.is_primary = False
                self.write_node(stored_node)
            if stored_node.name == node.name:
                stored_node.is_primary = True
                self.write_node(stored_node)

    def write_node(self, node: ClusterNode):
        node_path = pathlib.Path(self.storage_path, node.name)
        with node_path.open("w") as f:
            f.write(node.to_json())


class ClusterControl(StoppableThread):
    """A cluster control mechanism.

    Observe and maintain an app server cluster. Will load and build a cluster
    fresh from the storage and monitors cluster member nodes using UDP packet
    heartbeats. One member of cluster is elected as the primary node. If start
    and stop primary callbacks are provided, these are called as a node enters
    and exits primary member state.

    Application versions between node members are also monitored. The cluster
    has a version that equals the highest version number of node members. If a
    node detects that cluster version is higher than its own version, it will
    exit the cluster. If exit callback is provided, the callback will be run.

    Cluster control expects to receive a heartbeat from every other member of
    the cluster. If a sufficient amount of heartbeats are missed, the silent
    node is removed from the observed nodes. The cluster primary member will
    eventually remove the nodes from the database as well and a node will have
    to re-join.

    The cluster controller does not initially join the own node in the observed
    cluster, unless the cluster the cluster has already been started and has
    other running nodes in it (failsafe mechanism that re-joins a node if it
    drops out of a cluster). To ensure that a node is part of the cluster, the
    cluster.join_cluster method should be called separately from starting the
    controller.

    Basic usage:

    Starting and joining a cluster:
    >>> storage = SharedFsStorageBackend("/path/to/shared/folder")
    >>> ctrl = ClusterControl("labcluster", "127.0.0.1", "node01", 44330,
    >>>                       10, 15, 10, own_version="1.0",
    >>>                       storage_backend=storage)
    >>> join_cluster(ctrl)
    >>> ctrl.start()

    Stopping and exiting a cluster:
    >>> ctrl.stop()
    >>> ctrl.join(20)  # heartbeat timeout + whatever seconds it takes for a
    >>>                # node to exit gracefully
    >>> leave_cluster(ctrl)

    > Note:
    Note that the join timeout should be higher than the heartbeat timeout

    """
    cluster_name: str
    """A unique name to separate this cluster from others."""
    silent_nodes: dict
    """Key-value pairs of nodes that have gone silent (heartbeats gone missing). 
    Each key is the name of the cluster node and value is a `ClusterNode` 
    instance.
    """
    cluster: Cluster
    """An instance of the joined cluster."""
    hb_listener: HeartBeatListener
    """An instance of the heartbeat listener."""
    storage_backend: ClusterStorageBackend
    """A backend to store actual cluster node data in. 

    The storage can be anything that all cluster members have equal access to, 
    e.g a HA database or a storage server.
    """

    def __init__(self, cluster_name: str, host_name: str, node_name: str,
                 hb_port: int, hb_interval: int, hb_timeout: int,
                 check_interval: int, start_primary_callback: Callable = None,
                 stop_primary_callback: Callable = None,
                 exit_callback: Callable = None, hb_missed_count: int = 5,
                 own_version: str = None,
                 storage_backend: ClusterStorageBackend = None):
        """Create a new cluster controller.

        To start the controller, run the `start` method on the created object.
        This will start the controller and heartbeat in a separate thread. To
        stop the controller, run `stop`, followed by `join`. It is recommended
        to call `join_cluster` before starting the controller and `leave_cluster`
        after stopping the controller.

        Args:
            cluster_name: Name of cluster to observe
            host_name: Host name or IP address of own node
            node_name: Unique name for own node
            hb_port: Port number on which to start listening on heartbeats
            hb_interval: Amount of seconds between heartbeats
            hb_timeout: Amount of seconds to wait on heartbeat, in practice
                this should be considerably higher than the HB interval, to
                avoid false positives due to missed UDP packets
            check_interval: Amount of seconds between cluster health checks.
                Keeping this low ensures that cluster primary members are
                elected quicker, but may also stress the storage backend more
            start_primary_callback: Function reference that should be called
                when the node is elected as a primary member
            stop_primary_callback: Function reference that should be called
                when the node exits a primary member status
            exit_callback: Function reference that should be called when the
                node exits the cluster entirely on its own (e.g due to version
                incompatibility)
            hb_missed_count: Amount of heartbeats allowed to be missed
                until a node is cleared from the cluster, choose this based on
                how stable and reliable your network between the nodes is
            own_version: Software version string of this node. If set to None,
                the cluster will ignore version incompatibility between nodes.
                Accepts any string value that distutils.version.LooseVersion can
                parse
            storage_backend: A storage implementation to use

        """
        super().__init__()

        self.cluster_name = cluster_name
        self.node_name = node_name
        self.host = socket.gethostbyname(host_name)
        self.hb_port = hb_port
        self.hb_interval = hb_interval
        self.hb_timeout = hb_timeout
        self.hb_missed_count = hb_missed_count
        self.check_interval = check_interval
        self.silent_nodes = {}
        self.start_primary_callback = start_primary_callback or (lambda: None)
        self.stop_primary_callback = stop_primary_callback or (lambda: None)
        self.exit_callback = exit_callback or (lambda: None)
        self.ignore_versions = own_version is None
        self.own_version = own_version or "0"

        self.cluster = Cluster(self.hb_timeout)
        self.hb_clients = {}
        self.hb_listener = HeartBeatListener(self.host, self.hb_port,
                                             self.hb_interval, self.hb_timeout,
                                             self.cluster)
        self.log = logging.getLogger("ClusterControl")

        if storage_backend is None:
            raise RuntimeError(
                "Cannot join a cluster without a configured storage backend.")

        self.storage_backend = storage_backend
        self.storage_backend.set_cluster_name(self.cluster_name)

    @property
    def cluster_version(self) -> str:
        """Cluster version set during creation.

        If the cluster was created with the `ignore_versions` option, this will
        always return the string "0". Otherwise it is the highest software
        version within the cluster.
        """
        if self.ignore_versions:
            return "0"
        if self.cluster.version:
            return self.cluster.version
        return "0"

    def _clean_cluster(self, dead_nodes: List[ClusterNode]):
        """Update the storage and remove dead nodes."""
        self.storage_backend.remove_nodes([n.name for n in dead_nodes])

    def _get_cluster_nodes(self) -> List[ClusterNode]:
        """Get a list of cluster nodes from storage."""
        return self.storage_backend.get_all_nodes()

    def _load_cluster(self):
        """Update cluster members from the storage source.

        Will get cluster status fresh from storage and:
        - update known nodes with information from database
        - add any new node into list of monitored nodes
        - start heartbeats on the new nodes
        - remove any node that has left from monitored nodes
        - stop heartbeats on the departed nodes
        - detect which node is the own node
        - ensure that own node is still in the database (might have accidentally
            beeen removed by master if heartbeat is lagging), if the cluster
            still contains other active nodes. If there are no nodes left in the
            cluster, auto re-join will not take place, unless version
            compatibility has been set to be ignored

        """
        nodes = self._get_cluster_nodes()
        node_names = [n.name for n in nodes]

        new_nodes = [n for n in nodes if n.name not in self.cluster.nodes]
        old_nodes = [n for n in self.cluster.nodes.values() if n.name not in node_names]

        # update cluster object with up-to-date information from DB
        for node in nodes:
            self.cluster.update(node)

        # add new nodes to cluster, start sending heartbeat
        for node in new_nodes:
            self.log.info(f"{node} has joined cluster {self.cluster_name}")
            if node.name == self.node_name:
                self.log.debug(f"{node} is own node")
                node.is_self = True

            self.cluster.add(node)
            if not self.hb_clients.get(node.name) and not node.is_self:
                self.log.info(f"starting heartbeat client to send own node "
                              f"status to {node}")
                hb = HeartBeatClient(self.node_name, node.ip, node.port,
                                     self.hb_interval)
                self.hb_clients[node.name] = hb
                self.hb_clients[node.name].start()

        # remove nodes from cluster that are no longer in DB
        for node in old_nodes:
            self.log.info(f"{node} has left cluster {self.cluster_name}")
            if node.is_self and node.is_primary:
                self.log.warning(
                    f"{node} is own node and primary, yet removed by someone "
                    f"else: no longer safe to remain as primary")
                self.stop_primary_callback()
            self._remove_node(node)

        # ensure that own host is still in storage, if versions match
        if (self.node_name not in node_names and
                LooseVersion(self.cluster_version) == LooseVersion(self.own_version)):
            self.log.warning("own node has dropped out from cluster, rejoining")
            join_cluster(self)

    def _remove_node(self, node: ClusterNode):
        """Stop observing a node."""
        self.cluster.remove(node)
        if self.hb_clients.get(node.name):
            self.log.debug(f"stopping heartbeat for {node}")
            self.hb_clients[node.name].stop()
            self.hb_clients[node.name].join(1)
            del self.hb_clients[node.name]

    def _set_primary(self, node: ClusterNode):
        """Set selected node as primary."""
        self.storage_backend.set_primary(node)
        self.cluster.set_primary(node)

    def _validate_nodes(self):
        """Flag all known live nodes as active.

        Initially, all nodes join cluster in "inactive" state. The nodes are
        running and performing all tasks, but they are not considered to be
        active yet. As soon as a primary node has been elected, the primary
        node will flag all nodes that it has received heartbeats from as
        active.

        The purpose of the active flag is for external services, such as
        load balancers, to be aware of which cluster members can be "trusted"
        and which are ready to receive traffic.
        """
        for node in self.cluster.nodes.values():
            self.storage_backend.set_active(node)

    def run(self):
        timer = int(time.time())
        while True:
            if self.is_stopped:
                self.log.info("bringing down heartbeat senders")
                for node_name, hb in self.hb_clients.items():
                    self.log.debug(f"stopping heartbeat for {node_name}")
                    hb.stop()
                    hb.join(1)

                self.log.info("bringing down heartbeat listener")
                self.hb_clients = {}
                self.hb_listener.stop()
                self.hb_listener.join(self.hb_timeout + 1)
                break

            time.sleep(1)

            if int(time.time()) - timer >= self.check_interval:
                # ensure that cluster is up to date
                self._load_cluster()

                self_primary = False
                if self.cluster.own_node:
                    self_primary = self.cluster.own_node.is_primary

                if (self.cluster.version and
                        LooseVersion(self.cluster_version) > LooseVersion(self.own_version)):
                    self.log.warning("cluster is running newer version, exiting")
                    leave_cluster(self)
                    self.exit_callback()
                    self.stop()
                    continue

                # check for heartbeat loss
                dead_nodes = self.cluster.dead_nodes
                removed_nodes = []

                for node in dead_nodes:
                    self.log.debug(f"missed heartbeat from {node}")
                    # keep count of missed heartbeats
                    if node.name not in self.silent_nodes:
                        self.silent_nodes[node.name] = 0
                    else:
                        self.silent_nodes[node.name] += 1

                    # after hb_missed_count misses remove node
                    if self.silent_nodes[node.name] > self.hb_missed_count - 1:
                        self.log.info(
                            f"lost heartbeat from {node} ({self.hb_missed_count} "
                            f"missed beats), removing from cluster")
                        self._remove_node(node)
                        del self.silent_nodes[node.name]
                        removed_nodes.append(node)

                dead_node_names = [n.name for n in dead_nodes]
                woken_nodes = [n for n in self.silent_nodes
                               if n not in dead_node_names]

                for node_name in woken_nodes:
                    del self.silent_nodes[node_name]

                # check for primary status
                #
                # if no primary node is found, run election. If own node is
                # elected as primary, also clean storage from dead nodes and
                # validate all currently alive ones (set them to active in the
                # storage)
                if not self.cluster.primary_node:
                    self.log.info("cluster has no primary node, running election")
                    highest_node = self.cluster.oldest_node
                    if highest_node and highest_node.is_self:
                        self.log.info("electing self as primary since oldest node")
                        self._set_primary(highest_node)
                        if removed_nodes:
                            self.log.info("cleaning out dead nodes")
                            self._clean_cluster(dead_nodes)
                        self._validate_nodes()

                        self.start_primary_callback()

                elif (self_primary and
                      self.cluster.own_node != self.cluster.primary_node):
                    self.log.warning("another node has been selected as primary "
                                     "stopping primary node tasks")
                    self.stop_primary_callback()

                # run cluster cleanup tasks that only primary can do
                elif self.cluster.primary_node.is_self:
                    if removed_nodes:
                        self.log.info("cleaning out dead nodes")
                        self._clean_cluster(dead_nodes)
                    self._validate_nodes()

                timer = int(time.time())

    def start(self):
        """Start the cluster controller.

        Will load the cluster configuration and status from the storage and
        spawns a new thread that will observe the cluster status and performs
        cleanup routines. Also spawns a `HeartBeatClient` instance in a separate
        thread.

        """
        self.hb_listener.start()
        self._load_cluster()
        super().start()


class ClusterNode:
    """A single cluster node.

    Represents a node in the cluster. May be the own node, or any of the remote
    nodes that have also joined the cluster.

    There is no need to create `ClusterNode` instances manually, the nodes are
    created automatically when `ClusterControl.start` is called and when nodes
    join the cluster. The list of all nodes in the cluster can be retrieved as
    `ClusterControl.cluster.nodes`.
    """

    def __init__(self, node_name: str, node_ip: str, hb_port: int,
                 join_timestamp: datetime.datetime, version: str = "0",
                 primary: bool = False, active: bool = False, node_id: int = 0):
        self.last_hb: Optional[float] = None
        """UNIX timestamp with microseconds when last heartbeat was received."""
        self.name: str = node_name
        """Unique (within cluster) name for the node."""
        self.ip: str = node_ip
        """Node IP address."""
        self.port: int = hb_port
        """Port number on which the node listens for heartbeat."""
        self.join_timestamp: datetime.datetime = join_timestamp
        """Timestamp of when node joined the cluster."""
        self.version: str = version
        """Node software version number."""
        self.is_self = False
        """Indicates that this is the own node of the running application."""
        self.is_primary = True if primary else False
        """Indicates that the node is the primary node in the cluster."""
        self.is_active = True if active else False
        """Indicates that the node has been noted as being active."""
        self.id = node_id
        """An internal, unique and temporary ID assigned to the node when it 
        joined the cluster. May not change between leaving and joining the 
        cluster."""

    def __str__(self):
        if self.is_primary:
            return f"<node_{self.name}:{self.port} (primary)>"
        else:
            return f"<node_{self.name}:{self.port}>"

    def to_json(self) -> str:
        """Serialise storable Node data as a JSON string."""
        return json.dumps({
            "id": self.id,
            "ip": self.ip,
            "is_active": self.is_active,
            "is_primary": self.is_primary,
            "join_timestamp": datetime.datetime.strftime(self.join_timestamp, "%Y-%m-%d %H:%M:%S"),
            "name": self.name,
            "port": self.port,
            "version": self.version})

    @classmethod
    def from_json(cls, data: str) -> ClusterNode:
        """Generate a new ClusterNode instance from a serialised JSON string."""
        n = json.loads(data)
        return ClusterNode(
            n["name"], n["ip"], n["port"],
            datetime.datetime.strptime(n["join_timestamp"], "%Y-%m-%d %H:%M:%S"),
            n["version"], n["is_primary"], n["is_active"], n["id"])


class Cluster:
    """A cluster object.

    Collection of cluster nodes.

    There is no need to create a `Cluster` instance manually, it is created
    automatically when a `ClusterControl` instance is created and the currently
    active cluster can be retrieved as `ClusterControl.cluster`.
    """

    def __init__(self, hb_timeout: int):
        self._lock = threading.RLock()
        self._nodes = {}
        self._hb_timeout = hb_timeout

    def add(self, node: ClusterNode):
        """Add a node to the cluster.

        Usually not necessary to be run manually, nodes are automatically
        detected and added if `ClusterControl` is running.

        Args:
            node: `ClusterNode` instance
        """
        assert isinstance(node, ClusterNode), "node is not a ClusterNode"
        self._nodes[node.name] = node

    @property
    def dead_nodes(self) -> List[ClusterNode]:
        """All nodes that have exceeded the heartbeat timeout.

        Returns:
            A list of `ClusterNode` instances.
        """
        with self._lock:
            limit = time.time() - self._hb_timeout
            # nodes that are not ourselves, actually have a recorded heartbeat
            # and have heartbeat older than timeout are "silent"
            silent = [n for n in self._nodes.values()
                      if not n.is_self and n.last_hb is not None and n.last_hb < limit]

        return silent

    @property
    def nodes(self) -> dict:
        """All nodes currently in cluster.

        Returns:
            A dictionary containing name - `ClusterNode` instance pairs.
        """
        return self._nodes

    @property
    def oldest_node(self) -> Optional[ClusterNode]:
        """The oldest (highest ranking) cluster node.

        Node ranking is determined based on the join timestamp, the oldest being
        the highest ranking. If two or more nodes have joined the cluster at the
        same time, the order of joining is used to select the oldest member.

        Returns:
            A `ClusterNode` instance or None if there are no nodes in the
            cluster.
        """
        with self._lock:
            if self._nodes:
                oldest = min(iter(self._nodes.values()),
                             key=lambda x: x.join_timestamp)
                conflicts = [n for n in self.nodes.values()
                             if n.join_timestamp == oldest.join_timestamp]
                if len(conflicts) == 1:
                    return oldest
                else:
                    print("conflicting nodes, returning lowest id")
                    return min(conflicts, key=lambda x: x.id)
        return None

    @property
    def own_node(self) -> Optional[ClusterNode]:
        """Get own node.

        Returns:
            A `ClusterNode` instance or None if there are no nodes in the
            cluster.
        """
        with self._lock:
            for node in self._nodes.values():
                if node.is_self:
                    return node

        return None

    @property
    def primary_node(self) -> Optional[ClusterNode]:
        """Get current primary node.

        Returns:
            A `ClusterNode` instance or None if there are no nodes in the
            cluster or if there is no primary node.
        """
        with self._lock:
            for node in self._nodes.values():
                if node.is_primary:
                    return node

        return None

    def remove(self, node: ClusterNode):
        """Remove a named node from cluster.

        Not necessary to be run manually, cluster nodes are removed automatically
        if a `ClusterControl` instance is running. Removing a node manually may
        also result in the node being added back automatically.

        Args:
            node: The cluster node to remove
        """
        if self._nodes.get(node.name):
            del self._nodes[node.name]

    def update(self, node: ClusterNode):
        """Re-add a node to internal list of nodes.

        Should be run whenever a node change is detected. Done automatically if
        a `ClusterControl` instance is running.

        Args:
            node: The node instance to be re-added

        """
        if self._nodes.get(node.name):
            self._nodes[node.name].id = node.id
            self._nodes[node.name].ip = node.ip
            self._nodes[node.name].port = node.port
            self._nodes[node.name].join_timestamp = node.join_timestamp
            self._nodes[node.name].version = node.version
            self._nodes[node.name].is_primary = node.is_primary
            self._nodes[node.name].is_active = node.is_active

    def update_hb(self, node_name: str):
        """Update heartbeat for a named node.

        Resets the heartbeat timeout timer for the given node.

        Args:
            node_name: Node identifier

        """
        with self._lock:
            if not self._nodes.get(node_name):
                return
            self._nodes[node_name].last_hb = time.time()

    def set_primary(self, node: ClusterNode):
        """Flag a node as primary.

        Force a node to be primary. Running this manually is dangerous and may
        result in a split-brain scenario or multiple masters competing with
        each other.

        Args:
            node: Node to flag as primary

        """
        with self._lock:
            if self._nodes.get(node.name) and not self.primary_node:
                self._nodes[node.name].is_primary = True

    @property
    def version(self) -> Optional[str]:
        """Current cluster software version.

        The cluster node that is running the highest software version determines
        the entire cluster version - all nodes with lower version will drop out.

        Returns:
            String version number, or None if there are nodes in the cluster.

        """
        if self._nodes:
            return max([n.version for n in self._nodes.values()],
                       key=lambda x: LooseVersion(x))
        return None


class HeartBeatClient(StoppableThread):
    def __init__(self, sender_identity: str, hb_host: str, hb_port: int,
                 hb_interval: int):
        """Send heartbeat to a specific node.

        Heartbeat clients are initiated and started/stopped automatically if a
        `ClusterControl` instance is running.

        Args:
            sender_identity: Name of this host (will be sent to other nodes)
            hb_host: The IP address of the host to send heartbeat to
            hb_port: The heartbeat port number
            hb_interval: Aamount of seconds between heartbeats

        """
        super().__init__()

        self.sender = sender_identity
        self.host = hb_host
        self.port = hb_port
        self.interval = hb_interval

        self.log = logging.getLogger("HeartBeatClient")
        self.log.debug(f"starting notifying {hb_host}:{hb_port} with "
                       f"{hb_interval} second interval")

    def run(self):
        timer = int(time.time())
        while True:
            if self.is_stopped:
                break

            time.sleep(1)

            if int(time.time()) - timer >= self.interval:
                try:
                    packet = f"hb;{self.sender};{int(time.time())}"
                    hb = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    hb.sendto(packet.encode("utf-8"), (self.host, self.port))
                except Exception as e:
                    self.log.warning(f"failed to send heartbeat packet to "
                                     f"{self.host}:{self.port}: {e}")
                    print(e)
                timer = int(time.time())


class HeartBeatListener(StoppableThread):
    def __init__(self, hb_host: str, hb_port: int, check_interval: int,
                 hb_timeout: int, cluster_nodes: Cluster):
        """Receive heartbeats from other nodes.

        Will listen to heartbeats coming in from other cluster nodes and updates
        the last heartbeat timestamp for a node, when a heartbeat packet has
        been received.

        A listener is started automatically when `ClusterControl.start` is
        called.

        Args:
            hb_host: The IP address to listen incoming heartbeats on (own IP)
            hb_port: Port number to listen incoming heartbeats on
            check_interval: Amount of seconds to wait between each heartbeat
                check
            hb_timeout: Amount of seconds to wait on a heartbeat before giving
                up
            cluster_nodes: The `Cluster` instance that owns this listener

        """
        super().__init__()

        self.interval = check_interval
        self.cluster = cluster_nodes
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(hb_timeout)
        self.socket.bind((hb_host, hb_port))
        self.log = logging.getLogger("HeartBeatListener")
        self.log.info(f"starting listening to heartbeats on port {hb_port}")

    def run(self):
        if self.is_stopped:
            return

        while not self.is_stopped:
            ready, _, _ = select.select([self.socket], [], [], 0.5)
            if ready:
                try:
                    data, addr = self.socket.recvfrom(128)
                    data = data.decode()
                    addr = "{}:{}".format(*addr)
                    self.log.debug(f"got {data} from {addr}")
                    if data[:2] == "hb":
                        packet = data.split(";")
                        if len(packet) >= 3:
                            self.cluster.update_hb(packet[1])

                except Exception as e:
                    self.log.debug(f"socket error: {repr(e)}")


def join_cluster(cluster: ClusterControl):
    """Insert own node in cluster.

    Informs the cluster that a new member has joined.

    This method should be called before `ClusterControl.start` is called,
    otherwise the node will start in a "failed" state and will assume it has
    been kicked out of the cluster and may not rejoin automatically.

    Args:
        cluster: An instance of `ClusterControl` to join

    """
    new_node = ClusterNode(cluster.node_name, cluster.host, cluster.hb_port,
                           datetime.datetime.now(), cluster.own_version)

    if None in (new_node.name, new_node.ip, new_node.port):
        raise ValueError("node name, ip and HB port cannot be None")

    if new_node.version is None:
        new_node.version = "0"

    cluster.storage_backend.create_node(new_node)


def leave_cluster(cluster: ClusterControl):
    """Exit a cluster.

    Remove own node entry from the cluster tracking storage, resulting in
    heartbeats between this and other nodes stopping. Own node will continue to
    listen to heartbeats until `ClusterControl` has been stopped.

    > Note:
    Note that due to a failsafe rejoin mechanism in `ClusterControl`, the
    controller should be stopped before exiting a cluster.

    Args:
        cluster: Currently running `ClusterControl` instance.

    """
    cluster.storage_backend.remove_node(cluster.node_name)
