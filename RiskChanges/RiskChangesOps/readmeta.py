import pandas as pd
import psycopg2
def earmeta(connstr,earid):
    try: 
        df=pd.read_csv(connstr+'EarMeta.csv')#psycopg2.connect(connstr)
    except :
        print("unable to find EAR metadata table")
        raise ValueError("unable to find EAR metadata table")
    metatable=df[df.id==earid]
    assert not metatable.empty , f"The EAR id {earid} do not exists"
    return metatable

def classificationscheme(connstr,hazid):
    try: 
        df=pd.read_csv(connstr+'HazMeta.csv')#psycopg2.connect(connstr)
    except :
        print("unable to find Hazard Metadata")
        raise ValueError("unable to find metadata table")
    classtable=df[df.id==hazid]
    assert not classtable.empty , f"The classification scheme for hazard id  {hazid} do not exists"
    return classtable

def hazmeta(connstr,hazid):
    try: 
        df=pd.read_csv(connstr+'HazMeta.csv')
    except :
        print("unable to Hazard Metadata")
        raise ValueError("unable to find metadata table")
    metatable=df[df.id==hazid]
    assert not metatable.empty , f"The hazard id {hazid} do not exists"
    return metatable

def exposuremeta(connstr,exposureid):
    try: 
        df=pd.read_csv(connstr+'ExposureMeta.csv')
    except :
        print("unable to Exposure Metadata")
        raise ValueError("unable to find metadata table")
    metatable=df[df.id==exposureid]
    assert not metatable.empty , f"The exposure id {exposureid} do not exists"
    return metatable

def loadspprob(connstr,hazardid):
    try: 
        df=pd.read_csv(connstr+'SpatialProbability.csv')
    except :
        print("unable to Spatial Probability File")
        raise ValueError("unable to find metadata table")
    metatable=df[df.hazard_id==hazardid]
    assert not metatable.empty , f"The hazard id for spatial probability {hazardid} do not exists"
    return metatable

def computeloss_meta(connstr,exposureid):
    #get it from exposure
    meta=exposuremeta(connstr,exposureid)
    adminid=meta.admin_unit_id[0]
    exposureTable=meta.exposure_table[0]
    earID=meta.ear_index_id[0]
    hazid=meta.hazard_index_id[0]
    # earPK=meta.ear_index_primary_key[0]
    #get it from EAR
    metaear=earmeta(connstr,earID)
    Schema=metaear.filedir[0]
    earPK=metaear.data_id[0]
    # #ask tek?
    costColumn=metaear.col_value_avg[0]
    populColumn=metaear.col_population_avg[0]
    TypeColumn=metaear.col_class[0]
    #get it from hazard
    metahaz=hazmeta(connstr,hazid)
    hazunit=metahaz.unit[0]
    hazintensity=metahaz.intensity[0]
    base=metahaz.base_val[0]
    threshold=metahaz.threshold_val[0]
    stepsize=metahaz.interval_val[0]
    vulnColumn=metahaz.type[0]
    spprob=metahaz.sp_val[0]
    spprob_single=True
    if spprob==0:
        spprob=loadspprob(connstr,hazid)
        #spprob=spprob.set_index('sp_map_value')['sp'].to_dict()
        spprob_single=False
    lossmeta={'spprob':spprob,'Schema':Schema,'spprob_single':spprob_single,'exposureTable':exposureTable,'earID':earID,'hazid':hazid,'earPK':earPK,'costColumn':costColumn,'populColumn':populColumn,'TypeColumn':TypeColumn,'hazunit':hazunit,'hazintensity':hazintensity,'base':base,'stepsize':stepsize,'vulnColumn':vulnColumn,'threshold':threshold,'adminid':adminid}
    return lossmeta

def readLossMeta(connstr,lossid):
    try: 
        df=pd.read_csv(connstr+'LossMeta.csv')
    except :
        print("unable to Spatial Loss Metadata File")
        raise ValueError("unable to find metadata table")
    metatable=df[df.id==lossid]
    assert not metatable.empty , f"The loss id {lossid} do not exists"
    return metatable

def getReturnPeriod(connstr,lossid):
    lossmeta=readLossMeta(connstr,lossid)
    hazindex=lossmeta.hazard_index_id[0]
    metahaz=hazmeta(connstr,hazindex)
    return_period=metahaz.rp_avg[0]
    return return_period

def getHazardType(connstr,lossid):
    lossmeta=readLossMeta(connstr,lossid)
    hazindex=lossmeta.hazard_index_id[0]
    metahaz=hazmeta(connstr,hazindex)
    hazardType=metahaz.type[0]
    return hazardType

def getRiskMeta(connstr,riskid):
    try: 
        df=pd.read_csv(connstr+'RiskMeta.csv')
    except :
        print("unable to Spatial Risk Metadata File")
        raise ValueError("unable to find metadata table")
    metatable=df[df.id==riskid]
    assert not metatable.empty , f"The risk id {riskid} do not exists"
    return metatable
def getAdminMeta(connstr,adminid):
    try: 
        df=pd.read_csv(connstr+'AdminMeta.csv')
    except :
        print("unable to Spatial Admin Unit Metadata File")
        raise ValueError("unable to find metadata table")
    metatable=df[df.id==adminid]
    assert not metatable.empty , f"The admin unit id {adminid} do not exists"
    return metatable