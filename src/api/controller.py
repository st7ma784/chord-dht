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
        ]

   
    
