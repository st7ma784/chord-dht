



"""
Chord-DHT Package

This package implements a Chord Distributed Hash Table (DHT) for decentralized job management and execution. 
It is designed to distribute workloads across a network of nodes, ensuring scalability, fault tolerance, 
and efficient resource utilization.

## Why Distribute Workloads Between Chord Nodes?

The Chord DHT algorithm enables decentralized distribution of workloads by organizing nodes in a ring structure. 
Each node is responsible for a specific range of keys, allowing tasks to be distributed efficiently across the network. 
This approach is particularly beneficial for workloads like SuperDARN, which excel on single CPU cores but require 
scalable and fault-tolerant systems for large-scale data processing.

### Benefits for SuperDARN Workloads:
- **Scalability**: By distributing tasks across multiple nodes, the system can handle larger datasets and more complex computations.
- **Fault Tolerance**: The Chord DHT ensures redundancy and recovery mechanisms, minimizing the impact of node failures.
- **Efficient Resource Utilization**: Tasks are assigned to nodes based on their hash values, ensuring balanced workload distribution.

## How to Spin Up the System with Docker-Compose

To quickly set up the Chord-DHT system, you can use the provided `docker-compose.yml` file. Follow these steps:

1. **Install Docker and Docker-Compose**:
   Ensure that Docker and Docker-Compose are installed on your system. You can find installation instructions [here](https://docs.docker.com/get-docker/).

2. **Clone the Repository**:
   ```bash
   git clone https://github.com/st7ma784/chord-dht.git
   cd chord-dht
   ```
3. **Build the Docker Images**:
   ```bash
   docker-compose build
   ```
4. **Start the Chord-DHT Network**:
   ```bash
   docker-compose up
   ```
5. **Access the Web Interface**:
   Open your web browser and navigate to `http://localhost:5000` to access the Chord-DHT web interface.
6. **Submit Jobs**:
   Use the web interface to submit jobs to the Chord-DHT network. The controller will handle job distribution and execution across the nodes.
7. **Monitor Jobs**:
   You can monitor the status of your jobs through the web interface. The system will provide updates on job progress and completion.
   
"""
from .src import *