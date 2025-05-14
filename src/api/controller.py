import asyncio
from aiohttp import web
from .job import Job

"""
Controller Module
=================

This module defines the `ApiController` class, which provides an HTTP interface for interacting with a Chord 
Distributed Hash Table (DHT) network. The `ApiController` acts as the entry point for managing jobs, querying 
the Chord ring, and interacting with MinIO object storage.

Key Features
------------

- **Job Management**:
  - Submit jobs to the Chord DHT.
  - Retrieve the status of specific jobs or all jobs stored on the current node.
  - Distribute jobs across the Chord ring for decentralized execution.

- **MinIO Integration**:
  - Interact with MinIO object storage for bucket management.
  - Test MinIO connectivity and retrieve the list of available buckets.

- **Chord Ring Operations**:
  - Retrieve finger table entries for efficient routing.
  - Query the status of all nodes in the Chord ring.
  - Test the Chord DHT by submitting sample jobs.

- **System Status**:
  - Check the status of the MinIO server and the Chord DHT node.
  - Verify if the system is online and operational.

- **Web Interface**:
  - Serve an index page for the API's main interface.
  - Provide HTTP routes for interacting with the Chord DHT and MinIO.

Dependencies
------------

- **aiohttp**:
  Provides the framework for handling HTTP requests and responses.

- **asyncio**:
  Used for asynchronous operations and managing event loops.

- **MinIO**:
  Enables interaction with MinIO object storage for bucket and object management.

- **Job**:
  A custom class for managing distributed jobs in the Chord DHT.

Routes
------

The `ApiController` defines the following HTTP routes:

- `/submit`: Submit a job to the Chord DHT.
- `/getjob`: Retrieve the status of a specific job.
- `/getjobs`: Retrieve the status of all jobs stored on the current node.
- `/getbuckets`: Retrieve the list of MinIO buckets.
- `/getstatus`: Check the status of the MinIO server and Chord DHT node.
- `/getfinger`: Retrieve the finger table entries for the current node.
- `/nodes_status`: Retrieve information about all nodes in the Chord ring.
- `/test`: Test MinIO connectivity.
- `/test_dht`: Test the Chord DHT by submitting a sample job.
- `/`: Serve the index page for the API.

"""

class ApiController(asyncio.Protocol):
    """
    Represents the API controller, which acts as the entry point for HTTP requests.

    Provides endpoints for managing jobs, interacting with MinIO, and querying the Chord DHT network.
    """

    def __init__(self, chord_node):
        """
        Initializes the API controller with a reference to the Chord node.

        Args:
            chord_node: The Chord node instance this controller interacts with.
        """
        self.chord_node = chord_node
        self.jobs = {}  # Dictionary to store jobs submitted to the system

    async def index(self, request):
        """
        Serves the index.html file for the API's main page.

        This is the default route for the web interface.

        Args:
            request: The HTTP request object.

        Returns:
            web.FileResponse: The index.html file.
            web.Response: An error response if the file cannot be served.
        """
        try:
            return web.FileResponse('./src/api/templates/index.html')
        except Exception as e:
            print("Error: {}".format(e))
            return web.Response(text="Error: {}".format(e))

    async def test_minio(self, request):
        """
        Tests the connection to the MinIO object storage.

        If the "test" bucket does not exist, it creates the bucket.

        Args:
            request: The HTTP request object.

        Returns:
            web.Response: A response indicating the result of the test.
        """
        print("Testing Minio")
        bucket_name = "test"
        try:
            buckets = self.chord_node.MinioClient.list_buckets()
        except Exception as e:
            print("Error: {}".format(e))
            return web.Response(text="Error: {}".format(e))
        print("Buckets: {}".format(buckets))
        if bucket_name not in buckets:
            self.chord_node.MinioClient.make_bucket(bucket_name)
            print("Bucket {} created".format(bucket_name))

        return web.Response(text="Test Minio Done from node {} \n {}".format(self.chord_node._id, buckets))

    async def test_DHT(self, request):
        """
        Tests the Chord DHT by submitting a simple job.

        The job is distributed across the Chord ring, and the response indicates the nodes involved.

        Args:
            request: The HTTP request object.

        Returns:
            web.Response: A response indicating the result of the test.
        """
        print("Testing DHT")
        tasknm = "test"
        job_id = str(len(self.jobs) + 1)
        code_to_run = "echo 'Hello from DHT jobid {} launched from on node {}'".format(tasknm, self.chord_node._id)
        data = {"task": tasknm, "args": [code_to_run]}
        job = Job(job_id, data)
        self.jobs[job_id] = job
        # Submit the job to the Chord DHT
        response = await self.chord_node.put_job(job, ttl=3600)
        return web.Response(text="Test DHT web request processed from node {} \n given jobid: {} landing on nodes {}".format(self.chord_node._id, job_id, response))

    async def get_buckets(self, request):
        """
        Retrieves the list of buckets from MinIO.

        This is used to populate dropdowns in the web interface.

        Args:
            request: The HTTP request object.

        Returns:
            web.json_response: A JSON response containing the list of buckets.
            web.Response: An error response if the operation fails.
        """
        print("Getting Buckets")
        try:
            buckets = self.chord_node.MinioClient.list_buckets()
            print("Buckets: {}".format(buckets))
            response = {'buckets': [bucket.name for bucket in buckets]}
            return web.json_response(response)
        except Exception as e:
            print("Error: {}".format(e))
            return web.Response(text="Error: {}".format(e))

    async def add_job(self, request):
        """
        Adds a single job to the Chord DHT.

        The job is distributed to the appropriate node in the ring based on its hash.

        Args:
            request: The HTTP request object containing the job data.

        Returns:
            web.json_response: A JSON response containing the job ID and keys.
        """
        data = await request.json()
        job_id = str(len(self.jobs) + 1)
        print("received job request {}".format(data))
        job = Job(job_id, data)
        self.jobs[job_id] = job
        # Submit the job to the Chord DHT
        keys = await self.chord_node.put_job(job, ttl=3600)
        return web.json_response({'job_id': job_id, 'keys': keys})

    async def get_job_status(self, request):
        """
        Retrieves the status of a specific job by its hash.

        The job is looked up in the Chord DHT, and its status is returned.

        Args:
            request: The HTTP request object containing the job hash.

        Returns:
            web.json_response: A JSON response containing the job data or an error message.
        """
        job_id = request.query['hash']
        serial = await self.chord_node.find_job(job_id)
        if serial:
            job = Job.deserialize(serial)
            if job:
                return web.json_response(job.data)
        else:
            return web.json_response({'error': 'Job not found'}, status=404)

    async def get_all_jobs(self, request):
        """
        Retrieves the status of all jobs stored on the current node.

        This includes jobs that the node is responsible for in the Chord ring.

        Args:
            request: The HTTP request object.

        Returns:
            web.json_response: A JSON response containing the list of jobs.
        """
        jobs = []
        for key, val in zip(*self.chord_node._storage.get_my_data()):
            job = Job.deserialize(val)
            jobs.append({'server_idx': job.job_id, 'status': job.status, 'result': job.result, 'job_id': job.hash})
        response = {"jobs": jobs}
        return web.json_response(response)

    async def getStatus(self, request):
        """
        Retrieves the status of the MinIO server and the Chord DHT node.

        This is used to check if the system is online and operational.

        Args:
            request: The HTTP request object.

        Returns:
            web.json_response: A JSON response containing the system status.
        """
        try:
            status_dict = {"minio": "online" if len(self.chord_node.MinioClient.list_buckets()) >= 1 else "offline"}
        except Exception as e:
            status_dict = {"minio": "offline"}
        status_dict["chord"] = "online" if self.chord_node._predecessor is not None else "offline"
        status_dict["minioAddress"] = str(self.chord_node.minio_url.split(":")[0] + ":9001")
        return web.json_response(status_dict)

    async def getfinger(self, request):
        """
        Retrieves the finger table entries for the current Chord node.

        The finger table is used for efficient routing in the Chord ring.

        Args:
            request: The HTTP request object.

        Returns:
            web.json_response: A JSON response containing the finger table entries.
        """
        fingers = self.chord_node._fingers
        fingers = list(set(finger["addr"] for finger in fingers))
        return web.json_response({"finger": fingers})

    async def get_nodes(self):
        """
        Retrieves information about all nodes in the Chord ring.

        This includes node IDs, numeric IDs, and the keys they are responsible for.

        Returns:
            dict: A dictionary containing information about all nodes in the Chord ring.
        """
        response = {
            node: {
                "id": node.node_id,
                "numeric_id": node._numeric_id,
                "keys": node._storage.get_keys(node._predecessor["numeric_id"], node.node_id)
            }
            for node in self.chord_node._finger_table.values()
        }
        return response

    def get_routes(self):
        """
        Defines the HTTP routes for the API.

        These routes map to the methods defined in this controller.

        Returns:
            list: A list of web routes.
        """
        return [
            # Routes for job management
            web.get('/getjob', self.get_job_status),  # Get the status of a specific job
            web.get('/getjobs', self.get_all_jobs),  # Get the status of all jobs
            web.get('/nodes_status', self.get_nodes),  # Get the status of all nodes in the ring

            # Routes for submitting jobs
            web.post('/submit', self.add_job),  # Submit a single job to the system

            # Routes for interacting with MinIO
            web.get('/getbuckets', self.get_buckets),  # Get the list of MinIO buckets

            # Routes for system status and testing
            web.get('/getstatus', self.getStatus),  # Check if the system is online
            web.get('/getfinger', self.getfinger),  # Get the finger table entries
            web.get('/', self.index),  # Serve the index page
            web.get('/test', self.test_minio),  # Test MinIO integration
            web.get('/test_dht', self.test_DHT)  # Test the Chord DHT
        ]



