import geopandas as gpd
from . import readmeta
import psycopg2
import pandas as pd
import numpy as np


def readear(connstr, earid):
    metatable = readmeta.earmeta(connstr, earid)
    eartablename = metatable.filename[0]
    schema = metatable.filedir[0]
    filename=schema+'/'+eartablename
    try:
        ear_table = gpd.read_file(filename)
    except:
        ear_table = gpd.read_file(filename,geom_col='geometry')
        ear_table=ear_table.rename(columns ={'geometry':'geom'}).set_geometry('geom')
    ear_table=ear_table.rename_geometry('geom')
    return ear_table


def readexposure(connstr, exposureid, schema):
    metatable = readmeta.exposuremeta(connstr, exposureid)
    exposuretable = metatable.exposure_table[0]
    schema = metatable.filedir[0]
    filename=schema+'/'+exposuretable
    try:
        exposure_table = pd.read_csv(filename)
    except:
        print(f'could not find file {exposure_table}')
    return exposure_table


def readexposureGeom(connstr, exposureid):
    metadict = readmeta.computeloss_meta(connstr, exposureid)
    schema = metadict["filedir"]
    earid = metadict["earID"]
    pk = metadict["earPK"]
    exposuredata = readexposure(connstr, exposureid, schema)
    # joining on geom_id and pk so change it to joinincol
    eardatageom = readear(connstr, earid)
    eardatageom = eardatageom.rename(columns={pk: 'joining_id'})
    exposuredata = exposuredata.rename(columns={'geom_id': 'joining_id'})
    exposuredata_geom = eardatageom.merge(exposuredata, on='joining_id')
    return exposuredata_geom


def prepareExposureForLoss(connstr, exposureid):
    metadict = readmeta.computeloss_meta(connstr, exposureid)
    schema = metadict["Schema"]
    earid = metadict["earID"]
    pk = metadict["earPK"]
    exposuredata = readexposure(connstr, exposureid, schema)
    eardatageom = readear(connstr, earid)
    eardata = pd.DataFrame(eardatageom.drop(columns='geom'))
    eardata[pk]=eardata[pk].astype(np.int64)
    #print(eardata.dtypes)
    
    exposure_all = pd.merge(left=exposuredata, right=eardata, how='left', left_on=[
                            'geom_id'], right_on=[pk])
    assert not exposure_all.empty, f"The exposure data  {exposureid} returned empty from database"
    # exposure=readvulnerability.linkvulnerability(connstr,exposure_all)
    return exposure_all


def readLoss(connstr, lossid):
    # write more on loss reading
    metatable = readmeta.readLossMeta(connstr, lossid)
    losstable = metatable.loss_table[0]
    schema = metatable.filedir[0]  # TEK add workspace in loss index
    filename=schema+'/'+losstable
    try:
        loss_table = pd.read_csv(filename)
    except:
        print(f'Could not find loss table {filename}')
    assert not loss_table.empty, f"The Loss data  {lossid} returned empty from database"
    return loss_table


def readLossGeom(connstr, lossid):
    loss = readLoss(connstr, lossid)
    meta = readmeta.readLossMeta(connstr, lossid)
    earid = meta.ear_index_id[0]
    eardatageom = readear(connstr, earid)
    metataear = readmeta.earmeta(connstr, earid)
    pk = metataear.data_id[0]
    eardatageom = eardatageom.rename(columns={pk: 'joining_id'})
    loss = loss.rename(columns={'geom_id': 'joining_id'})
    lossdata_geom = eardatageom.merge(loss, on='joining_id')
    assert not lossdata_geom.empty, f"The loss data  {lossid} returned empty from database"
    return lossdata_geom


def readRiskGeom(connstr, riskid):
    meta = readmeta.getRiskMeta(connstr, riskid)
    earid = meta.earid[0]
    schema = meta.filedir[0]
    risktable = meta.risk_table[0]
    earmeta = readmeta.exposuremeta(connstr, earid)
    earPK = earmeta.data_id[0]
    filename=schema+'/'+risktable
    try:
        risk_table = pd.read_csv(filename)
    except:
        print(f'Could not find Risk table {filename}')
    ear = readear(connstr, earid)
    risk_table = pd.merge(left=risk_table, right=ear[[earPK, 'geom']], how='left', left_on=[
                          'Unit_ID'], right_on=[earPK], right_index=False)
    risk_table = gpd.GeoDataFrame(risk_table, crs=ear.crs, geometry='geom')
    return risk_table


def readAdmin(connstr, adminunit):
    meta = readmeta.getAdminMeta(connstr, adminunit)
    schema = meta.filedir[0]
    admintablename = meta.filename[0]
    adminpk = meta.data_id[0]
    filename=schema+'/'+admintablename
    try:
        ear_table = gpd.read_file(filename)
    except:
        ear_table = gpd.read_file(filename,geom_col='geometry')
        ear_table=ear_table.rename(columns ={'geometry':'geom'}).set_geometry('geom')
    # ear_table=ear_table.rename(columns={adminpk:'ADMIN_ID'})
    ear_table=ear_table.rename_geometry('geom')
    return ear_table