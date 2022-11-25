import pandas as pd
def writeexposuremeta(con,expid,filedir,exposure_table,earid,hazid,Ear_Table_PK,admin_unit):
    exp_meta_file=con+'ExposureMeta.csv'
    exposure_metafile=pd.read_csv(exp_meta_file)
    data=[{'id':expid,'filedir':filedir,'exposure_table':exposure_table,'ear_index_id':earid,'hazard_index_id':hazid,'ear_index_primary_key':Ear_Table_PK,'admin_unit_id':admin_unit}]
    exposure_metafile=exposure_metafile.append(data,ignore_index=True,sort=False)
    exposure_metafile.to_csv(con+'ExposureMeta.csv',index=False)
    print(f'Metadata Updated with length {len(exposure_metafile)}')
def writelossmeta(con,lossid,filedir,loss_table,admin_unit,earid,hazid,exposureid,computecol):
    loss_meta_file=con+'LossMeta.csv'
    loss_metafile=pd.read_csv(loss_meta_file)
    data=[{'id':lossid,'type':computecol,'filedir':filedir,'loss_table':loss_table,'ear_index_id':earid,'hazard_index_id':hazid,'exposure_index_id':exposureid,'admin_unit_id':admin_unit}]
    loss_metafile=loss_metafile.append(data,ignore_index=True,sort=False)
    loss_metafile.to_csv(con+'LossMeta.csv',index=False)
    print('Metadata Updated')
def writeriskmeta(con,riskid,filedir,risk_table,adminpk):
    loss_meta_file=con+'RiskMeta.csv'
    loss_metafile=pd.read_csv(loss_meta_file)
    data=[{'id':riskid,'filedir':filedir,'risk_table':risk_table,'admin_unit_id':adminpk}]
    loss_metafile=loss_metafile.append(data,ignore_index=True,sort=False)
    loss_metafile.to_csv(con+'LossMeta.csv',index=False)
    print('Metadata Updated')