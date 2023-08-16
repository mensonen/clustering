# An application clustering mechanism

This is a drop-in module that can be used to make any Python 3.7+ application
aware of its multiple concurrently running instances. 

The clustering works by:

 * Sending UDP heartbeats between application nodes, from a background thread
 * Running a maintenance background thread that periodically checks the health
    of the entire cluster
 * Using a central storage (shared filesystem, databases, etc) to record which
    nodes are currently part of the cluster
 * Electing one node as "primary"
 * Providing callback functions when a node becomes a primary, or loses its 
    primary status or when a node is instructed to shut down by the cluster

## View documentation

Install the [pdoc](https://pypi.org/project/pdoc/) package and run pdoc to view 
HTML-formatted API documentation in your browser:
```
~$ pip install pdoc
~$ pdoc -d google cluster.py
```

## Basic usage

1. From each application instance, create a cluster control instance:
    ```python
    import cluster
    
    def become_primary():
        """Function called when this node becomes primary."""
    def leave_primary():
        """Function called when this node stops being primary."""
    def forced_exit():
        """Function called when cluster wants this node to stop."""
    
    # path to a network share that every node can access
    shared_backend = cluster.SharedFsStorageBackend("/shared/storage/clustering")
    
    cluster_ctrl = cluster.ClusterControl(
        cluster_name="sample_cluster",  # name of cluster that everyone joins
        host_name="0.0.0.0",  # host address that every node can reach
        node_name="node01",  # our unique name
        hb_port=44330,  # UDP port for our heartbeat
        hb_interval=5,  # interval between sending heartbeats
        hb_timeout=10,  # seconds until a heartbeat from a node is considered as missed
        hb_missed_count=2,  # remove nodes after 2 misses (a bit low for production)
        own_version="1.0",  # node will exit if any node has a higher version
        check_interval=2,  # how often cluster health should be rechecked
        start_primary_callback=become_primary,
        stop_primary_callback=leave_primary,
        exit_callback=forced_exit,
        storage_backend=shared_backend)
    ```

2. From main thread of the application, let own node join the cluster
    ```python
    cluster.join_cluster(cluster_ctrl)
    ```
    
3. From main thread of the application, start the cluster control
    ```python
    cluster_ctrl.start()
    ```

4. When appplication stops, let cluster control finish all background 
activities and then gracefully exit the cluster:
    ```python
    cluster_ctrl.stop()
    cluster_ctrl.join(10)  # should be at least higher than the HB check interval
    cluster.leave_cluster(cluster_ctrl)
    ```

For more detailed usage instructions, refer to the `examples/` folder.

## Split-brain scenario handling

Rudimentary support for handling node isolation is available:

* When UDP packet network becomes entirely unavailable, but access to shared
  storage is available, all nodes will briefly drop out and re-join as-is. 
  Whoever was primary will remain primary.
* When primary node becomes isolated from the rest, but shared storage is still 
  available, other nodes will remove the primary and elect a new primary. The 
  old primary will automatically re-join and stop its primary functions.
* When shared storage becomes unavailable, the cluster state will freeze as it
  was and no updates to cluster will be possible. A manual restart of nodes is 
  required in order to restore clustering.
