import hashlib
import subprocess
from minio import Minio
from minio.error import S3Error
import pydarn
import matplotlib.pyplot as plt
import cv2
import base64
import json
import logging
import os
logger=logging.getLogger(__name__)
class Tasks:
    def getFitacfCommand(self, files, destfile,*args, **kwargs):
        return "make_fit -fitacf3 {} > {}".format(' '.join(files), destfile)
    def getDespeckCommand(self, files, destfile,*args, **kwargs):
        return "fit_speck_removal {} >{}".format(' '.join(files), destfile)
    def getCombineCommand(self, files, destfile,*args, **kwargs):
        return " cat {} > {}".format(' '.join(files), destfile)
    def getCombineGridCommand(self, files, destfile,*args, **kwargs):
        return "combine_grid {} > {}".format(' '.join(files), destfile)
    def getMakeGridCommand(self, files, destfile,*args, **kwargs):
        return "make_grid {} {} > {}".format(' '.join(files), kwargs.get('params', ''),destfile)
    def getMapGrdCommand(self, files, destfile,*args, **kwargs):
        return "map_grd {} | map_addhmb | map_addimf -if {} | map_addmodel {} | map_fit > {}".format(' '.join(files), kwargs.get('imffilepath', ''), kwargs.get('params', ''), destfile)
    def runCommand(self, files, destfile,*args, **kwargs):
        return " ".join(args)
    fitacf = getFitacfCommand
    despeck = getDespeckCommand
    combine = getCombineCommand
    combine_grid = getCombineGridCommand
    make_grid = getMakeGridCommand
    map_grd = getMapGrdCommand
    test=runCommand

class Job:
    @staticmethod
    def deserialize(string):
        # Deserialize the job from a string stored in the database
        #convert string to json and then to dictionary
        data = json.loads(string)    

        job = Job(data['job_id'], data['data'])
        job.hash = data['hash']
        job.status = data['status']
        job.result = data['result']
        job.tmpfile = data['tmpfile']
        job.destfile = data['destfile'] 
        return job

    def __init__(self, job_id, data):
        self.job_id = job_id
        self.data = data #data is a request.json()
        self.hash=hashlib.sha1(str(data).encode()).hexdigest()
        self.status = 'pending'
        self.result = None
        self.tmpfile='{}'.format(self.hash)
        self.destfile='{}'.format(self.hash)
        self.switcher = {
            'fitacf': Tasks.fitacf,
            'despeck': Tasks.despeck,
            'combine': Tasks.combine,
            'combine_grid': Tasks.combine_grid,
            'make_grid': Tasks.make_grid,
            'map_grd': Tasks.map_grd,
            'test': Tasks.test
        }
        self.VisualiseSwitcher={
            'fitacf': self.visualiseFitacf,
            'despeck': self.visualiseDespeck,
            'combine': self.visualiseCombine,
            'combine_grid': self.visualiseCombineGrid,
            'make_grid': self.visualiseMakeGrid,
            'map_grd': self.visualiseMapGrd
            
        }
        if self.data['task'] == 'test':
            self.run= self.run_test

    def serialize(self):
        # Serialize the job to a string to be stored in the database
        dictionary =  {
            'job_id': self.job_id,
            'data': self.data,
            'hash': self.hash,
            'tmpfile': self.tmpfile,
            'destfile': self.destfile,
            'status': self.status,
            'result': self.result
        }
        #convert dictionary to json, and dump to string
        text = json.dumps(dictionary)
        logger.info(f"Serialized job {self.job_id} to {text}")
        return text

    
    def run_test(self,node):
        cmd = self.switcher[self.data['task']]([], [], *self.data['args'])  

        os.system(cmd)
        os.system(" echo 'Running {} from DHT on node {}'".format(self.data['task'],node._id))
        self.status = 'completed'
        return 'completed'

    def run(self,node):
        # Implement the job logic here
        #from the data, extract objectname, source bucket, dest bucket and task and args
        #download the object
        MinioClient=node.MinioClient
        logger.info(f"Running job {self.job_id} in job")
        destfile=os.path.join('/dev/shm/',self.destfile)
        files=[]
        for inputfile in self.data["objectname"].split(","):
            
            tmpfile=os.path.join('/dev/shm/',self.tmpfile,inputfile)
                
            try:
                MinioClient.fget_object(self.data['source_bucket'], self.data['objectname'], tmpfile)
                if self.data['objectname'].endswith('.bz2'):
                    subprocess.run("bzip2 -d {}".format(tmpfile), shell=True)
                    tmpfile=tmpfile[:-4]

            except Exception as e:
                tmpfile=os.path.join('./',self.tmpfile)
                destfile=os.path.join('./',self.destfile)
                MinioClient.fget_object(self.data['source_bucket'], self.data['objectname'], tmpfile)
                if self.data['objectname'].endswith('.bz2'):
                    subprocess.run("bzip2 -d {}".format(tmpfile), shell=True)
                    tmpfile=tmpfile[:-4]
                    self.data['objectname']=self.data['objectname'][:-4]
            files.append(tmpfile)
        if self.data['task'] in self.switcher:
            cmd = self.switcher[self.data['task']](files, destfile, **self.data['args'])
            subprocess.run(cmd, shell=True)
        
            MinioClient.fput_object(self.data['dest_bucket'], self.data['objectname'], self.destfile)
        if self.data['task'] in self.VisualiseSwitcher:
            self.result = self.VisualiseSwitcher[self.data['task']](self.destfile)
        try:
            os.remove(self.tmpfile)
            os.remove(self.destfile)
        except:
            pass
        self.status = 'completed'
        return self.result

    def visualiseFitacf(self):
        fitacf_data = pydarn.SuperDARNRead(self.destfile).read_fitacf()

        fan_rtn = pydarn.Fan.plot_fan(fitacf_data, scan_index=27, 
                                colorbar_label='Velocity [m/s]')
        pydarn.Fan.plot_fan(fitacf_data,
                        scan_index=1, lowlat=60, zmin=-1000, zmax=1000,
                        boundary=True, radar_label=True,
                        groundscatter=True, ball_and_stick=True, len_factor=300,
                        coastline=True, parameter="v")
        path=self.destfile+'.png'
        plt.savefig(path)
        plt.close()
        data=cv2.imread(path)
        data = cv2.resize(data, (100, 100))
        os.remove(path)
        ret, buf = cv2.imencode('.png', data)
        return base64.b64encode(buf).decode('ascii')  
    
    def visualiseDespeck(self):
        fitacf_new = pydarn.SuperDARNRead(self.destfile, True).read_fitacf()
        fig, axs = plt.subplots(2, 4, figsize=(20, 10))
        for idx,data in enumerate([fitacf_new]):
            # Before filtering:
            pydarn.RTP.plot_summary(data, beam_num=7,
                                    range_estimation=pydarn.RangeEstimation.RANGE_GATE, ax=axs[0, 0])
            axs[idx, 0].set_title('Before Filtering - RTP Summary')

            pydarn.Fan.plot_fan(data, scan_index=10, coastline=True, ax=axs[0, 1])
            axs[idx, 1].set_title('Before Filtering - Fan Plot')

            # Evoke filter on data
            bx = pydarn.Boxcar(
                thresh=0.7,
                w=None
            )
            filtered_data = bx.run_filter(data)

            # After filtering
            pydarn.RTP.plot_summary(filtered_data, beam_num=7,
                                    range_estimation=pydarn.RangeEstimation.RANGE_GATE, ax=axs[1, 0])
            axs[idx, 2].set_title('After Filtering - RTP Summary')

            pydarn.Fan.plot_fan(filtered_data, scan_index=10, coastline=True, ax=axs[1, 1])
            axs[idx, 3].set_title('After Filtering - Fan Plot')

        plt.tight_layout()
        path=self.destfile+'.png'
        plt.savefig(path)
        plt.close()
        data=cv2.imread(path)
        data = cv2.resize(data, (100, 100))
        os.remove(path)
        ret, buf = cv2.imencode('.png', data)
        return base64.b64encode(buf).decode('ascii')

    def visualiseCombine(self):

        map_data = pydarn.SuperDARNRead().read_dmap(self.destfile)

        fig, axs = plt.subplots(2, 1, figsize=(10, 8))

        # First subplot
        pydarn.Maps.plot_mapdata(map_data, record=150, 
                            parameter=pydarn.MapParams.FITTED_VELOCITY,
                            lowlat=60, colorbar_label='Velocity m/s',
                            contour_fill=True,
                            contour_fill_cmap='RdBu',
                            contour_colorbar=True,
                            contour_colorbar_label='Potential (kV)',
                            pot_minmax_color='r',
                            map_info=True, imf_dial=True, hmb=True,
                            ax=axs[0])

        # Second subplot
        mlats = 75
        mlons = 110
        pydarn.Maps.plot_time_series(map_data,
                                    parameter=pydarn.MapParams.FITTED_VELOCITY,
                                    mlats=mlats, mlons=mlons,
                                    ax=axs[1])



        path=self.destfile+'.png'
        plt.savefig(path)
        plt.close()
        data=cv2.imread(path)
        data = cv2.resize(data, (100, 100))
        os.remove(path)
        ret, buf = cv2.imencode('.png', data)
        return base64.b64encode(buf).decode('ascii')  
    
    def visualiseCombineGrid(self):
        grid_data=pydarn.SuperDARNRead(self.destfile).read_grid()

        pydarn.Grid.plot_grid(grid_data,
                            colorbar_label='Velocity (m/s)',
                            radar_label=True, line_color='blue',
                            fov_color='grey')

        path=self.destfile+'.png'
        plt.savefig(path)
        plt.close()
        data=cv2.imread(path)
        data = cv2.resize(data, (100, 100))
        os.remove(path)
        ret, buf = cv2.imencode('.png', data)
        return base64.b64encode(buf).decode('ascii') 
    
    def visualiseMakeGrid(self):
         #Read in GRID file

        grid_data = pydarn.SuperDARNRead(self.destfile).read_grid()

        pydarn.Grid.plot_grid(grid_data,
                            colorbar_label='Velocity (m/s)',
                            radar_label=True, line_color='blue',
                            fov_color='grey')

        # Plots the field of views with gridded velocities on top!  
        plt.tight_layout()
        path=self.destfile+'.png'
        plt.savefig(path)
        plt.close()
        data=cv2.imread(path)
        data = cv2.resize(data, (100, 100))
        os.remove(path)
        ret, buf = cv2.imencode('.png', data)
        return base64.b64encode(buf).decode('ascii')  
    
    def visualiseMapGrd(self):
        map_data = pydarn.SuperDARNRead().read_dmap(self.destfile)

        fig, axs = plt.subplots(2, 1, figsize=(10, 8))

        # First subplot
        pydarn.Maps.plot_mapdata(map_data, record=150, 
                            parameter=pydarn.MapParams.FITTED_VELOCITY,
                            lowlat=60, colorbar_label='Velocity m/s',
                            contour_fill=True,
                            contour_fill_cmap='RdBu',
                            contour_colorbar=True,
                            contour_colorbar_label='Potential (kV)',
                            pot_minmax_color='r',
                            map_info=True, imf_dial=True, hmb=True,
                            ax=axs[0])

        # Second subplot
        mlats = 75
        mlons = 110
        pydarn.Maps.plot_time_series(map_data,
                                    parameter=pydarn.TimeSeriesParams.POT,
                                    potential_position=[mlons, mlats],
                                    ax=axs[1])

        plt.tight_layout()
        path=self.destfile+'.png'
        plt.savefig(path)
        plt.close()
        data=cv2.imread(path)
        data = cv2.resize(data, (100, 100))
        os.remove(path)
        ret, buf = cv2.imencode('.png', data)
        return base64.b64encode(buf).decode('ascii')  

    