import hashlib
import subprocess
import pydarn
import matplotlib.pyplot as plt
import cv2
import base64
import json
import logging
import asyncio
from tqdm import tqdm
from collections import defaultdict
from typing import List, Tuple
import datetime
import os

logger = logging.getLogger(__name__)

class Tasks:
    """
    This class defines shell commands for various tasks.
    Each method generates a command string for a specific task.
    """
    def getFitacfCommand(files, destfile, *args, **kwargs):
        return "make_fit -fitacf3 {} > {}".format(' '.join([str(f) for f in files]), destfile)

    def getDespeckCommand(files, destfile, *args, **kwargs):
        return "fit_speck_removal {} >{}".format(' '.join([str(f) for f in files]), destfile)

    def getCombineCommand(files, destfile, *args, **kwargs):
        try:
            print("combine {} > {}".format(' '.join([str(f) for f in files]), destfile))
            return " cat {} > {}".format(' '.join([str(f) for f in files]), destfile)
        except Exception as e:
            print("Error combining files: {}".format(e))
            raise e

    def getCombineGridCommand(files, destfile, *args, **kwargs):
        return "combine_grid {} > {}".format(' '.join([str(f) for f in files]), destfile)

    def getMakeGridCommand(files, destfile, *args, **kwargs):
        return "make_grid {} {} > {}".format(' '.join([str(f) for f in files]), kwargs.get('params', ''), destfile)

    def getMapGrdCommand(files, destfile, *args, **kwargs):
        return "map_grd {} | map_addhmb | map_addimf -if {} | map_addmodel {} | map_fit > {}".format(
            ' '.join([str(f) for f in files]), kwargs.get('imffilepath', ''), kwargs.get('params', ''), destfile)

    def runCommand(files, destfile, *args, **kwargs):
        return " ".join([str(f) for f in files])

    # Task aliases for easier access
    fitacf = getFitacfCommand
    despeck = getDespeckCommand
    combine = getCombineCommand
    combine_grid = getCombineGridCommand
    make_grid = getMakeGridCommand
    map_grd = getMapGrdCommand
    test = runCommand


class NameConverters:
    """
    This class provides methods to convert filenames for different tasks.
    It ensures that filenames are transformed appropriately for each task.
    """
    def convertFitacfName(inputFileName):
        return inputFileName.replace('.rawacf', '.fitacf3').replace('.bz2', '')

    def convertDespeckName(inputFileName):
        return inputFileName.replace('.fitacf3', '.despeck.fitacf3').replace('.bz2', '')

    def converttoDailyName(inputFileName):
        object_names = inputFileName.split(",")
        file = object_names[0].split("/")[-1]
        result = object_names[0].replace(file, str(file[:8]) + "." + str(file.split(".")[3]) + "." + ".".join([str(f) for f in file.split(".")[4:]]))
        return result.replace('.bz2', '')

    def combineGridName(inputFileName):
        object_names = inputFileName.split(",")
        return object_names[0].replace(object_names[0].split("/")[-1], str(object_names[0].split("/")[-1][:8]) + ".north.grd")

    def makeGridName(inputFileName):
        return inputFileName.replace('.fitacf3', '.grd').replace('.bz2', '').replace('.despeck', '')

    def mapGrdName(inputFileName):
        return inputFileName.replace('.grd', '.map')

    def runName(inputFileName):
        return inputFileName


class Job:
    """
    Represents a job in the Chord DHT system.
    Each job is responsible for:
    - Selecting files from MinIO based on the task.
    - Downloading the files to local storage.
    - Running the specified command.
    - Uploading the results back to MinIO.
    """

    @staticmethod
    def deserialize(string):
        """
        Deserialize a job from a JSON string.
        """
        data = json.loads(string)
        job = Job(data['job_id'], data)
        return job

    def __init__(self, job_id, data):
        """
        Initialize a job with its ID and data.
        """
        self.job_id = job_id
        self.data = data  # Job data (e.g., task, source bucket, etc.)
        self.data.update({'job_id': job_id})
        self.hash = data.get("hash", hashlib.sha1(str(data).encode("utf-8")).hexdigest())
        if "hash" not in data:
            self.data.update({"hash": self.hash})
        self.status = data.get('status', 'pending')
        self.result = data.get('result', None)

        # Define task-specific behavior
        self.switcher = {
            'fitacf': Tasks.fitacf,
            'despeck': Tasks.despeck,
            'combine': Tasks.combine,
            'combine_grid': Tasks.combine_grid,
            'make_grid': Tasks.make_grid,
            'map_grd': Tasks.map_grd,
            'test': Tasks.test
        }

        self.ObjectNameConverters = {
            'fitacf': NameConverters.convertFitacfName,
            'despeck': NameConverters.convertDespeckName,
            'combine': NameConverters.converttoDailyName,
            'combine_grid': NameConverters.combineGridName,
            'make_grid': NameConverters.makeGridName,
            'map_grd': NameConverters.mapGrdName,
            'test': NameConverters.runName
        }

    def serialize(self):
        """
        Serialize the job to a JSON string.
        """
        self.data.update({'status': self.status})
        self.data.update({'result': self.result})
        self.data.update({'hash': self.hash})
        self.data.update({'job_id': self.job_id})
        text = json.dumps(self.data)
        logger.info(f"Serialized job {self.job_id} to {text}")
        return text

    async def run(self, node):
        """
        Execute the job logic:
        1. Select files from MinIO based on the task.
        2. Download the files to local storage.
        3. Run the specified command.
        4. Upload the results back to MinIO.
        """
        if self.status == "completed":
            return self.result

        elif self.status == "running":
            MinioClient = node.MinioClient
            destfile = os.path.join('/dev/shm/', self.ObjectNameConverters[self.data['task']](self.data['objectname']))
            os.makedirs(os.path.dirname(destfile), exist_ok=True)

            # Step 1: Download files from MinIO
            files = []
            for inputfile in self.data["objectname"].split(','):
                tmpfile = os.path.join('/dev/shm/', inputfile.split("/")[-1])
                try:
                    MinioClient.fget_object(self.data['source_bucket'], inputfile, tmpfile)
                    if inputfile.endswith('.bz2'):
                        subprocess.run(f"bzip2 -d {tmpfile}", shell=True)
                        tmpfile = tmpfile[:-4]
                except Exception as e:
                    logger.error(f"Error downloading file {inputfile}: {e}")
                    raise e
                files.append(tmpfile)

            # Step 2: Run the task command
            if self.data['task'] in self.switcher:
                cmd = self.switcher[self.data['task']](files, destfile, *self.data.get('args', []))
                subprocess.run(cmd, shell=True)

            # Step 3: Upload results back to MinIO
            MinioClient.fput_object(self.data['dest_bucket'], self.ObjectNameConverters[self.data['task']](self.data['objectname']), destfile)

            # Step 4: Clean up temporary files
            for file in files:
                os.remove(file)
            os.remove(destfile)

            self.status = 'completed'
            return self.result

