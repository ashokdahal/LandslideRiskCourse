import pandas as pd
import numpy as np
import numpy.ma as ma
import geopandas as gpd
import rasterio
import os 
def reclassify(in_image, out_image, base, stepsize, maxval):
    input_image = rasterio.open(in_image)
    intensity_data = input_image.read(1)

    prev = base
    thresholds = np.arange(start=base, stop=maxval+1, step=stepsize).tolist()
    intensity_data[intensity_data < base] = input_image.nodata
    intensity_data_classified = np.copy(intensity_data)

    for i, threshold in enumerate(thresholds):
        #mean=intensity_data[((intensity_data<threshold) & (intensity_data>=prev))].mean()

        intensity_data_classified[(
            (intensity_data < threshold) & (intensity_data >= prev))] = i

        prev = threshold

        # if it is the last value, need to assign the max class for all result
        if threshold == thresholds[-1]:
            intensity_data_classified[(
                intensity_data >= thresholds[-1])] = i

    with rasterio.Env():
        profile = input_image.profile
        with rasterio.open(out_image, 'w', **profile) as dst:
            dst.write(intensity_data_classified, 1)
        dst = None
    input_image = None

def ClassifyHazard(hazard_file, base, stepsize, threshold):
    infile = hazard_file
    outfile = hazard_file.replace(".tif", "_reclassified.tif")
    if os.path.isfile(outfile):
        pass
    else:
        reclassify(infile, outfile, base, stepsize, threshold)
    return outfile
