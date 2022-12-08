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

## Split-brain scenario

The clustering mechanism has no support for multiple heartbeat mechanisms. In a
scenario where UDP heartbeat becomes available, all cluster nodes will deselect
themselves as primary. If also the shared storage becomes available, the cluster
ndoes will shut down.
