import asyncio
from aiohttp import web
from .job import Job
import logging
logger=logging.getLogger(__name__)

class ApiController(asyncio.Protocol):
    """
    This class represent the API controller (The entry class).
    """

    def __init__(self, chord_node):
        self.chord_node = chord_node
        self.jobs = {}
        
    async def index(self, request):
        return web.Response(text="Welcome to the API server {}:{}".format(self.chord_node._id,self.chord_node._numeric_id))
    
    async def test_minio(self, request):
        logger.info("Testing Minio")
        bucket_name = "test"
        object_name = "test.txt"
        data = "Hello World from node {}".format(self.chord_node._id)

        buckets=self.chord_node.MinioClient.list_buckets()
        logger.info("Buckets: {}".format(buckets))
        if bucket_name not in buckets:
            self.chord_node.MinioClient.make_bucket(bucket_name)
            logger.info("Bucket {} created".format(bucket_name))

        return web.Response(text="Test Minio Done from node {} \n {}".format(self.chord_node._id,buckets))
    
    async def add_job(self, request):
        data = await request.json()
        job_id = str(len(self.jobs) + 1)
        logger.info("received job request {}".format(data))
        job = Job(job_id, data)
        self.jobs[job_id] = job
        # Logic to move job to relevant worker
        logger.info(f"Adding job {job_id} to chord node")

        await self.chord_node.put_job(job,ttl=10)
        logger.info("Job {} added to chord".format(job_id))
        return web.json_response({'job_id': job_id})

    async def get_job_status(self, request):
        job_id = request.match_info['job_id']
        job = self.jobs.get(job_id)
        if job:
            return web.json_response({'status': job.status, 'result': job.result})
        else:
            return web.json_response({'error': 'Job not found'}, status=404)

    def get_routes(self):
        return [
            web.post('/jobs', self.add_job),
            web.get('/jobs/{job_id}', self.get_job_status),
            #add plain index page
            web.get('/', self.index),
            web.get('/test', self.test_minio)
        ]

   
    
