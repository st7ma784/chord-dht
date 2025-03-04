import hashlib
import subprocess
from minio import Minio
from minio.error import S3Error
import pydarn
import matplotlib.pyplot as plt
import cv2
import base64

import os
class Tasks:
    def getFitacfCommand(self, tmpfile, destfile,*args, **kwargs):
        return "make_fit -fitacf3 {} > {}".format(tmpfile, destfile)
    def getDespeckCommand(self, tmpfile, destfile,*args, **kwargs):
        return "fit_speck_removal {} >{}".format(tmpfile, destfile)
    def getCombineCommand(self, tmpfile, destfile,*args, **kwargs):
        return " cat {} > {}".format(tmpfile, destfile)
    def getCombineGridCommand(self, tmpfile, destfile,*args, **kwargs):
        return "combine_grid {} > {}".format(tmpfile, destfile)
    def getMakeGridCommand(self, tmpfile, destfile,*args, **kwargs):
        return "make_grid {} {} > {}".format(tmpfile, destfile, kwargs.get('params', ''))
    def getMapGrdCommand(self, tmpfile, destfile,*args, **kwargs):
        return "map_grd {} | map_addhmb | map_addimf -if {} | map_addmodel {} | map_fit > {}".format(tmpfile, kwargs.get('imffilepath', ''), kwargs.get('params', ''), destfile)
    fitacf = getFitacfCommand
    despeck = getDespeckCommand
    combine = getCombineCommand
    combine_grid = getCombineGridCommand
    make_grid = getMakeGridCommand
    map_grd = getMapGrdCommand


class Job:
    def __init__(self, job_id, data):
        self.job_id = job_id
        self.data = data
        self.hash=hashlib.sha1(data).hexdigest()[: 8//4]
        self.status = 'pending'
        self.result = None
        self.tmpfile='/dev/shm/{}'.format(self.hash)
        self.destfile='/dev/shm/{}'.format(self.hash)
        self.switcher = {
            'fitacf': Tasks.fitacf,
            'despeck': Tasks.despeck,
            'combine': Tasks.combine,
            'combine_grid': Tasks.combine_grid,
            'make_grid': Tasks.make_grid,
            'map_grd': Tasks.map_grd
        }
        self.VisualiseSwitcher={
            'fitacf': self.visualiseFitacf,
            'despeck': self.visualiseDespeck,
            'combine': self.visualiseCombine,
            'combine_grid': self.visualiseCombineGrid,
            'make_grid': self.visualiseMakeGrid,
            'map_grd': self.visualiseMapGrd
        }
    def run(self,MinioClient):
        # Implement the job logic here
        #from the data, extract objectname, source bucket, dest bucket and task and args
        #download the object
        MinioClient.fget_object(self.data['source_bucket'], self.data['objectname'], self.tmpfile)
        if self.data['task'] in self.switcher:
            cmd = self.switcher[self.data['task']](self.tmpfile, self.destfile, **self.data['args'])
            subprocess.run(cmd, shell=True)
        MinioClient.fput_object(self.data['dest_bucket'], self.data['objectname'], self.destfile)
        self.result = self.VisualiseSwitcher[self.data['task']](self.destfile)
        os.remove(self.tmpfile)
        os.remove(self.destfile)
        self.status = 'completed'

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

    