import asyncio
from aiohttp import web
from .job import Job

"""
This module defines the ApiController class which provides an HTTP interface for interacting with a Chord DHT (Distributed Hash Table) network.
The ApiController allows users to submit jobs, check job status, interact with Minio storage, and manage buckets.
Jobs are distributed across the Chord nodes, leveraging the DHT for decentralized job management and execution.
"""
class ApiController(asyncio.Protocol):
    """
    This class represent the API controller (The entry class).
    """

    def __init__(self, chord_node):
        self.chord_node = chord_node
        self.jobs = {}
        
    async def index(self, request):
        #return index.html file
        try:
            return web.FileResponse('./src/api/templates/index.html')
        except Exception as e:
            print("Error: {}".format(e))
            return web.Response(text="Error: {}".format(e))
        # return web.Response(text="Welcome to the API server {}:{}".format(self.chord_node._id,self.chord_node._numeric_id))
    
    async def test_minio(self, request):
        print("Testing Minio")
        bucket_name = "test"
        try:
            buckets=self.chord_node.MinioClient.list_buckets()
        except Exception as e:
            print("Error: {}".format(e))
            return web.Response(text="Error: {}".format(e))
        print("Buckets: {}".format(buckets))
        if bucket_name not in buckets:
            self.chord_node.MinioClient.make_bucket(bucket_name)
            print("Bucket {} created".format(bucket_name))

        return web.Response(text="Test Minio Done from node {} \n {}".format(self.chord_node._id,buckets))
    
    async def test_DHT(self, request):
        print("Testing DHT")
        tasknm = "test"
        job_id = str(len(self.jobs) + 1)
        code_to_run = "echo 'Hello from DHT jobid {} launched from on node {}'".format(tasknm,self.chord_node._id)
        data={"task":tasknm,"args":[code_to_run]}
        job = Job(job_id, data)
        self.jobs[job_id] = job
        # Logic to move job to relevant worker
        response= await self.chord_node.put_job(job,ttl=3600)
        return web.Response(text="Test DHT web request processed from node {} \n given jobid: {} landing on nodes {}".format(self.chord_node._id,job_id, response))

    async def get_buckets(self, request):
        print("Getting Buckets")
        try:
            buckets=self.chord_node.MinioClient.list_buckets()
            print("Buckets: {}".format(buckets))
            response={'buckets': []}
            for bucket in buckets:
                response['buckets'].append(bucket.name)
            return web.json_response(response)
                
        except Exception as e:
            print("Error: {}".format(e))
            return web.Response(text="Error: {}".format(e))
    
    async def add_job(self, request):
        data = await request.json()
        job_id = str(len(self.jobs) + 1)
        print("received job request {}".format(data))
        job = Job(job_id, data)
        self.jobs[job_id] = job
        # Logic to move job to relevant worker
        # print(f"Adding job {job_id} to chord node")
        keys= await self.chord_node.put_job(job,ttl=3600)
        # print("Job {} added to chord with location: ".format(job_id,keys))
        return web.json_response({'job_id': job_id, 'keys': keys})

    async def get_job_status(self, request):
        # print(request.keys())
        job_id = request.query['hash']
        serial=await self.chord_node.find_job(job_id)
        if serial:
            job=Job.deserialize(serial)

        #To Do: look up jobs in the chord node as well and return the status

            if job:
                return web.json_response(job.data)
        else:
            return web.json_response({'error': 'Job not found'}, status=404)
        
    async def get_all_jobs(self, request):
        #To Do: look up jobs in the chord node as well and return the status
        jobs=[]
        for key,val in zip(*self.chord_node._storage.get_my_data()):
            job=Job.deserialize(val)
            jobs.append({'server_idx':job.job_id,'status': job.status, 'result': job.result, 'job_id':job.hash})
        response = {"jobs":jobs}
        return web.json_response(response)
    
    async def getStatus(self, request):
        try:
            status_dict={"minio": "online" if len(self.chord_node.MinioClient.list_buckets())>=1 else "offline"}
        except Exception as e:
            status_dict={"minio":"offline"}
        if self.chord_node._predecessor is not None:
            status_dict["chord"]="online"
        else:
            status_dict["chord"]="offline"
        status_dict["minioAddress"]=str(self.chord_node.minio_url.split(":")[0]+":9001")
        return web.json_response(status_dict)
    
    async def getfinger(self, request):
        ''' returns a list of finger table entries'''
        fingers=self.chord_node._fingers
        fingers=list(set(fingers["addr"] for fingers in fingers))
        return web.json_response({"finger":fingers})
    
    async def get_nodes(self):
        response={node:{"id":node.node_id,"numeric_id":node._numeric_id, "keys":node._storage.get_keys(node._predecessor["numeric_id"], node.node_id) } for node in self.chord_node._finger_table.values()}
        return response
    
    def get_routes(self):
        return [
            #routes for interactive funtionalities
            web.get('/getjob', self.get_job_status), # will be used by loading bar per job
            web.get('/getjobs', self.get_all_jobs), # will be used by container for seeing running jobs
            web.get('/nodes_status', self.get_nodes), # will be used by container for showing node status w/ network info
            #routes for submitting jobs - either bulk - or single 
            # submit buckets is a bulk job, that will trigger many other jobs being added to the system
            # add_job is a single job, that will trigger a single job being added to the system, rarely used in practice           
            web.post('/submit', self.add_job), # will be used to submit jobs to the system

            #Basic routes for page navigation
            web.get('/getbuckets', self.get_buckets),       #Used by dropdown in index.html
            web.get('/getstatus', self.getStatus),      #Used for just checking we're online
            web.get('/getfinger', self.getfinger),      #Used for just checking we're online
            web.get('/', self.index),
            web.get('/test', self.test_minio),
            web.get('/test_dht', self.test_DHT)
        ]

   
    
