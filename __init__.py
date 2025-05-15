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

Why Use This System?
--------------------

For the purpose of the SuperDARN project, we need to distribute workloads between Chord nodes to ensure that tasks are executed efficiently and reliably. 
It also aligns with the goal of eventually performing machine learning on the data, which requires a distributed system to handle large datasets and complex computations.

This codebase assumes a MinIO server to store the data, which is a distributed object storage system compatible with Amazon S3. MinIO excels at random access to data, 
such as might be needed for machine learning tasks, and is designed to work well with distributed systems. This allows for efficient data retrieval and processing, 
making it an ideal choice for the SuperDARN project.

However, as seen in the PTL part of the project, caching is still a useful speed-up for the system.

For future users who may have LUNA or other systems, the code is designed to be modular and extensible but may not be as efficient as can be implemented 
in a more tightly integrated storage backend.

To future RSEs: This can probably be improved by using a more efficient storage backend, such as LUNA or other systems, but was sufficient for the current use case.

Benefits for SuperDARN Workloads
--------------------------------

- **Scalability**: By distributing tasks across multiple nodes, the system can handle larger datasets and more complex computations.
- **Fault Tolerance**: The Chord DHT ensures redundancy and recovery mechanisms, minimizing the impact of node failures.
- **Efficient Resource Utilization**: Tasks are assigned to nodes based on their hash values, ensuring balanced workload distribution.

How to Spin Up the System with Docker-Compose
----------------------------------------------

To quickly set up the Chord-DHT system, you can use the provided `docker-compose.yml` file. Follow these steps:

1. **Install Docker and Docker-Compose**:
   Ensure that Docker and Docker-Compose are installed on your system. You can find installation instructions `here <https://docs.docker.com/get-docker/>`_.

2. **Clone the Repository**:
   .. code-block:: bash

      git clone https://github.com/st7ma784/chord-dht.git
      cd chord-dht

3. **Build the Docker Images**:
   .. code-block:: bash

      docker-compose build

4. **Start the Chord-DHT Network**:
   .. code-block:: bash

      docker-compose up

5. **Access the Web Interface**:
   Open your web browser and navigate to `http://localhost:5000` to access the Chord-DHT web interface.

6. **Submit Jobs**:
   Use the web interface to submit jobs to the Chord-DHT network. The controller will handle job distribution and execution across the nodes.

7. **Monitor Jobs**:
   You can monitor the status of your jobs through the web interface. The system will provide updates on job progress and completion.
"""
# import src



import os
import sys
#run pip install -r requirements.txt
os.system("pip install -r requirements.txt")