import asyncio
from aiohttp import web
from .service import ApiService
from .job import Job


class ApiController:
    """
    This class represent the API controller (The entry class).
    """

    def __init__(self, chord_node):
        self.chord_node = chord_node
        self.jobs = {}
       
    async def add_job(self, request):
        data = await request.json()
        job_id = str(len(self.jobs) + 1)
        job = Job(job_id, data)
        self.jobs[job_id] = job
        # Logic to move job to relevant worker
        await self.chord_node.put_job(job)
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
