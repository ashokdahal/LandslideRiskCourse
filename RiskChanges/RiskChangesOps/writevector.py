import psycopg2
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine


def writeexposure(df, filedir, filename):
    fullname=filedir+'/'+filename
    df.to_csv(fullname)
    print(f'Exposure data is written at {fullname}')


def writeexposureAgg(df, filedir, filename):
    fullname=filedir+'/'+filename+'_AGG'
    df.to_csv(fullname)


def writeLossAgg(df, connstr, schema):
    fullname=filedir+'/'+filename+'_AGG'
    df.to_csv(fullname)
    print(f'Loss data is written at {fullname}')


def writeLoss(df, filedir, filename):
    fullname=filedir+'/'+filename
    print(f'Loss data is written at {fullname}')
    df.to_csv(fullname)


def writeRisk(df, filedir, filename):
    fullname=filedir+'/'+filename
    print(f'Risk data is written at {fullname}')
    df.to_csv(fullname)

def writeRiskAgg(df, filedir, filename):
    fullname=filedir+'/'+filename+'_AGG'
    df.to_csv(fullname)
    print(f'Risk data is written at {fullname}')


def writeRiskCombined(df, filedir, filename):
    fullname=filedir+'/'+filename
    print(f'Risk data is written at {fullname}')
    df.to_csv(fullname)

