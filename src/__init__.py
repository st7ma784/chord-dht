"""
Chord-DHT Package
=================

This package implements a Chord Distributed Hash Table (DHT) for decentralized job management and execution. 
It is designed to distribute workloads across a network of nodes, ensuring scalability, fault tolerance, 
and efficient resource utilization.

Why Distribute Workloads Between Chord Nodes?
---------------------------------------------

The Chord DHT algorithm enables decentralized distribution of workloads by organizing nodes in a ring structure. 
Each node is responsible for a specific range of keys, allowing tasks to be distributed efficiently across the network. 
This approach is particularly beneficial for workloads like SuperDARN, which excel on single CPU cores but require 
scalable and fault-tolerant systems for large-scale data processing.

Benefits for SuperDARN Workloads
--------------------------------

- **Scalability**: By distributing tasks across multiple nodes, the system can handle larger datasets and more complex computations.
- **Fault Tolerance**: The Chord DHT ensures redundancy and recovery mechanisms, minimizing the impact of node failures.
- **Efficient Resource Utilization**: Tasks are assigned to nodes based on their hash values, ensuring balanced workload distribution.

File Organization
------------------

The Chord-DHT package is organized into two main components:

1. **chord**:
   - Contains the core logic of the Chord DHT system, including the implementation of the ring structure, hashing, and node communication.

2. **api**:
   - Acts as the web interface for nodes, enabling external interactions and handling requests, such as those from SuperDARN.

This separation ensures a clear distinction between the core functionality and the interface layer, promoting modularity and ease of maintenance.

How to Spin Up the System with Docker-Compose
----------------------------------------------

To quickly set up the Chord-DHT system, you can use the provided `docker-compose.yml` file. Follow these steps:

1. **Install Docker and Docker-Compose**:
   Ensure that Docker and Docker-Compose are installed on your system. You can find installation instructions `here <https://docs.docker.com/get-docker/>`_.

2. **Clone the Repository**:
   .. code-block:: bash

      git clone https://github.com/st7ma784/chord-dht.git
      cd chord-dht
"""
# import .api

# import .chord

# from main import _start_api_server, _stop_api_server, _start_chord_node, _start