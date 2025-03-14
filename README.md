# Chord DHT Web Server

This repository contains an implementation of a Chord Distributed Hash Table (DHT) for a web server. The Chord DHT is used for decentralized job management and execution, leveraging a distributed network of nodes.

## Features

- **Distributed Job Management**: Submit and manage jobs across a network of Chord nodes.
- **Minio Integration**: Interact with Minio storage for bucket management.
- **HTTP API**: Provides an HTTP interface for interacting with the Chord DHT network.

## Getting Started

### Prerequisites

- Python 3.7+
- Docker 
- `aiohttp` library
- `asyncio` library
- Minio server (optional, for storage integration)

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/st7ma784/chord-dht.git
    cd chord-dht
    ```

2. Install the required dependencies:
    ```sh
    docker compose up -d
    ```

## API Endpoints

- `GET /`: Returns the index page.
- `GET /test_minio`: Tests the Minio integration.
- `GET /test_DHT`: Tests the DHT functionality.
- `GET /buckets`: Retrieves the list of buckets from Minio.
- `POST /add_job`: Submits a new job to the Chord network.
- `GET /job_status/{job_id}`: Retrieves the status of a specific job.
- `GET /all_jobs`: Retrieves the status of all jobs.
- `POST /submit_buckets`: Submits a bucket-related job.
- `GET /status`: Retrieves the status of the Chord node and Minio.
- `GET /finger`: Retrieves the finger table entries of the Chord node.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [Chord DHT](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)
- [aiohttp](https://docs.aiohttp.org/en/stable/)
- [Minio](https://min.io/)
