"""
API Package

This package contains modules and classes for implementing and interacting with 
a Chord Distributed Hash Table (DHT) network.

The package is split into two main components:

1. **Controller**:
   - Handles the web interface for the Chord DHT network.
   - When a web request is received, the controller launches a job that is distributed across the compute nodes in the Chord ring.
   - The controller acts as a workload manager, ensuring that tasks are distributed efficiently and executed on the appropriate nodes.
   - This design is analogous to workload managers on HPC systems, where parallel submissions are used to distribute tasks across multiple compute nodes.

2. **Job**:
   - Handles job submission and management, enabling tasks to be executed remotely across the Chord DHT network.
   - The `job.py` file contains the `Job` class, which defines the logic for submitting and managing jobs.
   - Each job instance can trigger additional tasks across all nodes in the network, ensuring scalability and efficient resource utilization.

### Job Logic in the Controller

The controller is responsible for receiving web requests and translating them into jobs that can be executed across the Chord DHT network. When a request is received:
1. A job is created and submitted to the Chord ring.
2. The job is run, and if necessary, submits more jobs.
3. The job is distributed to the appropriate node(s) based on the hash of the job's key.
4. The workload is executed on the assigned nodes, leveraging the distributed nature of the Chord DHT.

This approach ensures that workloads are balanced across the network and that resources are utilized efficiently. On HPC systems, this can be thought of as analogous to parallel job submissions, where tasks are distributed across multiple compute nodes for execution.

### Summary

The API package provides the tools necessary to interact with the Chord DHT network, enabling efficient workload distribution and execution. The combination of the controller and job logic ensures that tasks are managed effectively, making this system a powerful workload manager for distributed computing environments.
"""
from controller import ApiController
from job import Job,Tasks, NameConverters
