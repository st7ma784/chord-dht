import hashlib
import subprocess
import pydarn
import matplotlib.pyplot as plt
import cv2
import base64
import json
import logging
from collections import defaultdict
from typing import List, Tuple
import datetime
import os
logger=logging.getLogger(__name__)
class Tasks:
    def getFitacfCommand(files, destfile,*args, **kwargs):
        return "make_fit -fitacf3 {} > {}".format(' '.join(files), destfile)
    def getDespeckCommand(files, destfile,*args, **kwargs):
        return "fit_speck_removal {} >{}".format(' '.join(files), destfile)
    def getCombineCommand(files, destfile,*args, **kwargs):
        return " cat {} > {}".format(' '.join(files), destfile)
    def getCombineGridCommand(files, destfile,*args, **kwargs):
        return "combine_grid {} > {}".format(' '.join(files), destfile)
    def getMakeGridCommand(files, destfile,*args, **kwargs):
        return "make_grid {} {} > {}".format(' '.join(files), kwargs.get('params', ''),destfile)
    def getMapGrdCommand(files, destfile,*args, **kwargs):
        return "map_grd {} | map_addhmb | map_addimf -if {} | map_addmodel {} | map_fit > {}".format(' '.join(files), kwargs.get('imffilepath', ''), kwargs.get('params', ''), destfile)
    def runCommand(files, destfile,*args, **kwargs):
        return " ".join(args)
    fitacf = getFitacfCommand
    despeck = getDespeckCommand
    combine = getCombineCommand
    combine_grid = getCombineGridCommand
    make_grid = getMakeGridCommand
    map_grd = getMapGrdCommand
    test=runCommand

class NameConverters:
    #This is a class for how to convert filenames from input to output, as a crude way of ensuring sensible methods are called in the right order
    def convertFitacfName(inputFileName):
        return inputFileName.replace('rawacf','.fitacf3').replace('.bz2','')
    def convertDespeckName(inputFileName):
        return inputFileName.replace('fitacf3','.despeck.fitacf3').replace('.bz2','')
    def converttoDailyName(inputFileName):
        object_names=inputFileName.split(",")   
        #take just the date part of the filename
        file=object_names[0].split("/")[-1]
        return object_names[0].replace(file,file[:8]+"."+file.split(".")[3]+file.split(".")[4:])
    def combineGridName(inputFileName):
        object_names=inputFileName.split(",")
        return object_names[0].replace(object_names[0].split("/")[-1],object_names[0].split("/")[-1][:8]+".north.grd")
    def makeGridName(inputFileName):
        return inputFileName.replace('.fitacf3','grd').replace('.bz2','').replace('.despeck','')
    def mapGrdName(inputFileName):
        return inputFileName.replace('.grd','.map')
    def runName(inputFileName):
        return inputFileName
    
class Visualizers:
    #This is a class for how to visualize the output of the tasks
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
    
class FileGroupers:
    #this is a class for how to yield files from a bucket

    radarnames=['bks','cly','cvw','fhe','fhw','gbr','han','hok','inv','kap','ker','kod','ksr','lyr','pgr','pyk','rkn','sas','sto','sye','sys','tig','wal','zho']
    
    def singleFiles(bucket,node):
        total_files=len(list(node.MinioClient.list_objects(bucket)))
        for idx, file in enumerate(node.MinioClient.list_objects(bucket)):
            yield ([file.object_name], idx / total_files)
    def groupByRadarAndDate(bucket,node):
        '''pools files with the same date and radar name together'''
        total_files=len(list(node.MinioClient.list_objects(bucket)))
        RadarDate_default_dict = defaultdict(lambda: defaultdict(set))
        yielded_files = 0
        for idx, file in enumerate(node.MinioClient.list_objects(bucket)):
            radar_name = next((name for name in FileGroupers.radarnames if name in file), None)
            if radar_name:
                file_date = file.split(".")[0][:8]
                RadarDate_default_dict[radar_name][file_date].add(file)
                if len(RadarDate_default_dict[radar_name]) > 8:
                    yielded_files += 1
                    yield list(RadarDate_default_dict[radar_name]), yielded_files / total_files
        for radar_name in RadarDate_default_dict:
            for date in RadarDate_default_dict[radar_name]:
                yielded_files += 1
                yield list(RadarDate_default_dict[radar_name][date]), yielded_files / total_files
        
    def groupByDate(bucket,node):
        '''pools files with the same date together'''
        min_date=datetime.datetime.now().timestamp()
        max_date=0
        for idx, file in enumerate(node.MinioClient.list_objects(bucket)):
            file_date=int(file.split(".")[0])
            if file_date>max_date:
                max_date=file_date
            if file_date<min_date:
                min_date=file_date
        total_num_days=(max_date-min_date)/(24*3600)
        Date_default_dict=defaultdict(set)
        yielded_files=0
        for idx, file in enumerate(node.MinioClient.list_objects(bucket)):
            Date_default_dict[file.split(".")[0][:8]].add(file)
            if len(Date_default_dict)>8:
                yielded_files+=1
                yield list(Date_default_dict),yielded_files/total_num_days
        for entry in Date_default_dict:
            yielded_files+=1
            yield list(Date_default_dict[entry]),yielded_files/total_num_days
                
    def groupByHour(bucket,node) :
        '''pools files with the same hour together'''
        min_date = datetime.datetime.now().timestamp()
        max_date = 0
        for file, idx in node.MinioClient.list_objects(bucket):
            file_date = int(file.split(".")[0])
            if file_date > max_date:
                max_date = file_date
            if file_date < min_date:
                min_date = file_date
        total_num_hours = (max_date - min_date) / 3600
        Hour_default_dict = defaultdict(set)
        yielded_files = 0
        for idx, file in enumerate(node.MinioClient.list_objects(bucket)):
            file_hour = file.split(".")[0][:10]  # Assuming the hour is included in the filename
            Hour_default_dict[file_hour].add(file)
            if len(Hour_default_dict) > 24:
                yielded_files += 1
                yield list(Hour_default_dict), yielded_files / total_num_hours
        for entry in Hour_default_dict:
            yielded_files += 1
            yield list(Hour_default_dict[entry]), yielded_files / total_num_hours
    def groupByRadarAndHour(bucket,node):
        '''pools files with the same hour and radar name together'''
        min_date = datetime.datetime.now().timestamp()
        max_date = 0
        for file, idx in node.MinioClient.list_objects(bucket):
            file_date = int(file.split(".")[0])
            if file_date > max_date:
                max_date = file_date
            if file_date < min_date:
                min_date = file_date
        total_num_hours = (max_date - min_date) / 3600
        Hour_default_dict = defaultdict(set)
        yielded_files = 0
        for idx, file in enumerate(node.MinioClient.list_objects(bucket)):
            radar_name = next((name for name in FileGroupers.radarnames if name in file), None)
            if radar_name:
                file_hour = file.split(".")[0][:10]  # Assuming the hour is included in the filename
                Hour_default_dict[radar_name][file_hour].add(file)
                if len(Hour_default_dict[radar_name]) > 24:
                    yielded_files += 1
                    yield list(Hour_default_dict[radar_name]), yielded_files / total_num_hours
        for radar_name in Hour_default_dict:
            for entry in Hour_default_dict[radar_name]:
                yielded_files += 1
                yield list(Hour_default_dict[radar_name][entry]), yielded_files / total_num_hours
 

class Job:
    @staticmethod
    def deserialize(string):
        # Deserialize the job from a string stored in the database
        #convert string to json and then to dictionary
        data = json.loads(string)    

        job = Job(data['job_id'], data['data'])
        return job

    def __init__(self, job_id, data):
        self.job_id = job_id
        self.data = data #data is a request.json()
        self.hash=hashlib.sha1(str(data).encode()).hexdigest()
        self.status = 'pending'
        self.result = None
        #To Do - these are probably all wrong!
        self.file_grouper = {
            'fitacf': FileGroupers.singleFiles,
            'despeck': FileGroupers.singleFiles,
            'convert_to_daily': FileGroupers.groupByDate,
            'combine_grids': FileGroupers.singleFiles,
            'make_grid': FileGroupers.singleFiles,
            'map_grd': FileGroupers.singleFiles,
            'test': FileGroupers.singleFiles
        }



        self.switcher = {
            'fitacf': Tasks.fitacf,
            'despeck': Tasks.despeck,
            'convert_to_daily': Tasks.combine,
            'combine_grids': Tasks.combine_grid,
            'make_grid': Tasks.make_grid,
            'map_grd': Tasks.map_grd,
            'test': Tasks.test
        }
        self.VisualiseSwitcher={
            'fitacf': Visualizers.visualiseFitacf,
            'despeck': Visualizers.visualiseDespeck,
            'convert_to_daily': Visualizers.visualiseCombine,
            'combine_grids': Visualizers.visualiseCombineGrid,
            'make_grid': Visualizers.visualiseMakeGrid,
            'map_grd': Visualizers.visualiseMapGrd
            
        }
        self.ObjectNameConverters={
            'fitacf': NameConverters.convertFitacfName,
            'despeck': NameConverters.convertDespeckName,
            'convert_to_daily': NameConverters.converttoDailyName,
            'combine_grids': NameConverters.combineGridName,
            'make_grid': NameConverters.makeGridName,
            'map_grd': NameConverters.mapGrdName,
            'test': NameConverters.runName
        }

        if self.data['task'] == 'test':
            self.run= self.run_test
        elif self.data['task'] == 'read_from_luna':
            self.run=self.Luna_store_job
        elif self.data.get('launch',False):
            self.run=self.task_launcher
        else:
            self.tmpfile='tmp{}{}'.format(self.hash[:4],self.data.get('objectname',''))
            self.destfile='dest{}{}'.format(self.hash[:4],self.ObjectNameConverters[self.data['task']](self.data.get('objectname','')))


    def serialize(self):
        # Serialize the job to a string to be stored in the database
        dictionary =  {
            'job_id': self.job_id,
            'data': self.data,
            'hash': self.hash,
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

    def Luna_store_job(self,node):
        #this will be the job for running the python script to copy data from minio to LUNA. 
        #``
        print("running luna store job")
        lunapath=self.data['lunapath']
        source_bucket=self.data['source_bucket']
        minio_path=self.data['minio_path']
        user=self.data['user']
        password=self.data['password']
        ##to do : find a way to spread this round the hash table too! 
        Processed=0
        try:
            #mount luna
            smbclient.ClientConfig(username=user, password=password, domain="luna.lancs.ac.uk")
            smbclient.register_session(r"\\luna.lancs.ac.uk\FST", username=user, password=password)
            #read all files in the directory and upload them to the minio bucket
            logger.info(f"Uploading files from {lunapath} to {source_bucket}")
            total_files= len(smbclient.listdir(lunapath))

            for root, dirs, files in smbclient.walk(lunapath):
                for file in files:
                    lunapath=os.path.join(root, file)
                    with open(lunapath, 'rb') as f:
                        minio_client.put_object(source_bucket, os.path.join(minio_path,os.path.basename(lunapath)), f, length=-1)
                        Processed+=1
            #unmount luna
            smbclient.unmount("//luna.lancs.ac.uk/FST")
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            pass
        self.status = 'completed'
        return f"Processed {Processed} out of {total_files} files"
    def task_launcher(self,node):
        '''
        This is the task for the job. It is called by the worker node to run the job. 
        It will take an input and output bucket, 
        step1:
        search files in the input bucket
        step2: 
        group according to task
        step3:
        put the groups of files in the worker nodes for processing
        step4:
        return progress and status of the job 
        step5:
        keep tabs on job ids
        '''
        print(f"Running job {self.job_id} in job launcher")
        print(f"Data: {self.data}")
        #step1 
        self.status = 'running'
        bucket=self.data['source_bucket']
        #step2
        for idx,(files,progress) in enumerate(self.file_grouper[self.data['task']](bucket,node)):
            #group files according to task
            data=self.data.copy()
            data['objectname']=', '.join(files)
            data['launch']=False
            node.put_job(Job(str(int(self.job_id)+idx),data),ttl=3600)
            self.status=progress
        self.status = 'completed'
        print(f"Job {self.job_id} completed")
        return 'completed'
    
    def run(self,node):
        # Implement the job logic here
        #from the data, extract objectname, source bucket, dest bucket and task and args
        #download the object
        MinioClient=node.MinioClient
        print(f"Running job {self.job_id} in job")
        destfile=os.path.join('/dev/shm/',self.destfile)
        files=[]
        for inputfile in self.data["objectname"].split(","):
            
            tmpfile=os.path.join('/dev/shm/',self.tmpfile,inputfile)
                
            try:
                MinioClient.fget_object(self.data['source_bucket'], self.data['objectname'], tmpfile)
                if inputfile.endswith('.bz2'):
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
            cmd = self.switcher[self.data['task']](files, destfile, *self.data['args'])
            subprocess.run(cmd, shell=True)
        
            MinioClient.fput_object(self.data['dest_bucket'], self.ObjectNameConverters[self.data['task']](self.data['objectname']), self.destfile)
        if self.data['task'] in self.VisualiseSwitcher:
            self.result = self.VisualiseSwitcher[self.data['task']](self)
        try:
            os.remove(self.tmpfile)
            os.remove(self.destfile)
        except:
            pass
        self.status = 'completed'
        return self.result

