# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 10:20:28 2023

@author: kamalesh.mandhyan
"""

import pandas as pd
import os
import csv    
import time
import datetime
import datetime as dt
from datetime import date
today = date.today() 

os.chdir(r"D:\Manit\IDFC")
os.getcwd()
print("job started at : " + str(datetime.datetime.time(datetime.datetime.now())))

deinstall1 = pd.read_csv(r'D:\Manit\IDFC\Input\De-Instalaltion.csv',encoding='latin1')
deployment1 = pd.read_csv(r'D:\Manit\IDFC\Input\Deployment Sheet.csv', encoding='latin1')
asset1 = pd.read_csv(r'D:\Manit\IDFC\Input\Tid Asset Report.csv', encoding='latin1')
replacement1 = pd.read_csv(r'D:\Manit\IDFC\Input\Tid Replacement Sheet.csv', encoding='latin1')
fixed = pd.read_csv(r'D:\Manit\IDFC\Input\TID_Status_Fix_File.csv', encoding='latin1')
permanent = pd.read_csv(r'D:\Manit\IDFC\Input\Permanant_TID_FIX.csv', encoding='latin1')
master = pd.read_excel(r'D:\Manit\IDFC\Input\IDFC bank Merchant Signup-Master.xlsb',sheet_name ='Main sheet',engine='pyxlsb')
invalid = pd.read_excel(r'D:\Manit\IDFC\Input\IDFC bank Merchant Signup-Master.xlsb',sheet_name ='Invalid Dump',engine='pyxlsb')

master['IDFC/FD Request Date (DD-MMM-YY)'] = master['IDFC/FD Request Date (DD-MMM-YY)'].astype(str)
master['Transaction'] = master['IDFC/FD Request Date (DD-MMM-YY)'].str.contains(pat = '41')
opening_incorrect_date_trns = master[master['Transaction'] == False]
opening_correct_date_trns = master[master['Transaction'] != False]
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('float64')
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('int64')
opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'] = pd.to_datetime(opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'], errors='coerce',unit='d',origin='1900-01-01')
master_1 = opening_incorrect_date_trns.append(opening_correct_date_trns)

master_1['IDFC/FD Request Date (DD-MMM-YY)'] = master_1['IDFC/FD Request Date (DD-MMM-YY)'].astype(str)
master_1['Transaction'] = master_1['IDFC/FD Request Date (DD-MMM-YY)'].str.contains(pat = '42')
opening_incorrect_date_trns = master_1[master_1['Transaction'] == False]
opening_correct_date_trns = master_1[master_1['Transaction'] != False]
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('float64')
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('int64')
opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'] = pd.to_datetime(opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'], errors='coerce',unit='d',origin='1900-01-01')
master_2 = opening_incorrect_date_trns.append(opening_correct_date_trns)

master_2['IDFC/FD Request Date (DD-MMM-YY)'] = master_2['IDFC/FD Request Date (DD-MMM-YY)'].astype(str)
master_2['Transaction'] = master_2['IDFC/FD Request Date (DD-MMM-YY)'].str.contains(pat = '43')
opening_incorrect_date_trns = master_2[master_2['Transaction'] == False]
opening_correct_date_trns = master_2[master_2['Transaction'] != False]
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('float64')
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('int64')
opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'] = pd.to_datetime(opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'], errors='coerce',unit='d',origin='1900-01-01')
master_3 = opening_incorrect_date_trns.append(opening_correct_date_trns)

master_3['IDFC/FD Request Date (DD-MMM-YY)'] = master_3['IDFC/FD Request Date (DD-MMM-YY)'].astype(str)
master_3['Transaction'] = master_3['IDFC/FD Request Date (DD-MMM-YY)'].str.contains(pat = '44')
opening_incorrect_date_trns = master_3[master_3['Transaction'] == False]
opening_correct_date_trns = master_3[master_3['Transaction'] != False]
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('float64')
opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"] = opening_correct_date_trns["IDFC/FD Request Date (DD-MMM-YY)"].astype('int64')
opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'] = pd.to_datetime(opening_correct_date_trns['IDFC/FD Request Date (DD-MMM-YY)'], errors='coerce',unit='d',origin='1900-01-01')
master_4 = opening_incorrect_date_trns.append(opening_correct_date_trns)

master_5 = pd.DataFrame()
master_5 = master_4[master_4['IDFC/FD Request Date (DD-MMM-YY)']!='nan']

master_5['Plutus TID '] = master_5['Plutus TID '].astype(str)
permanent['Plutus TID '] = permanent['Plutus TID '].astype(str)
permanent.rename(columns={'Status':'Permanent Status'},inplace=True)
permanent.rename(columns={'TID Installation Date':'Permanent TID Installation Date'},inplace=True)
permanent.rename(columns={'De-Installation Date':'Permanent De-Installation Date'},inplace=True)

final_1 = pd.merge(master_5,permanent,how='left',on='Plutus TID ')
final_1['Permanent Status'] = final_1['Permanent Status'].fillna('NA')
final_1['Permanent TID Installation Date'] = final_1['Permanent TID Installation Date'].fillna('NA') 
final_1['Permanent De-Installation Date'] = final_1['Permanent De-Installation Date'].fillna('NA')

final_1['Plutus TID '] = final_1['Plutus TID '].astype(str)
fixed['Plutus TID '] = fixed['Plutus TID '].astype(str)
fixed.rename(columns={'Status':'Fix Status'},inplace=True)
fixed.rename(columns={'TID Installation Date':'Fix TID Installation Date'},inplace=True)
fixed.rename(columns={'De-Installation Date':'Fix De-Installation Date'},inplace=True)

final_2 = pd.merge(final_1,fixed,how='left',on='Plutus TID ')
final_2['Fix Status'] = final_2['Fix Status'].fillna('NA')
final_2['Fix TID Installation Date'] = final_2['Fix TID Installation Date'].fillna('NA') 
final_2['Fix De-Installation Date'] = final_2['Fix De-Installation Date'].fillna('NA')

#def per_status(c):
#    if c['Permanent Status'] == 'NA':
#        response = c['Fix Status']
#    else:
#        response = c['Permanent Status']
#    return response
#final_2['Permanent Status'] = final_2.apply(per_status,axis=1)

asset1.sort_values(by = ['Root Asset: LMS POS ID'], ascending = False, inplace=True)
asset = asset1[['TID','Root Asset: LMS POS ID','Parent POS ID','Status']]
asset.rename(columns = {'TID' : 'Asset TID'}, inplace = True)
asset.rename(columns = {'Status' : 'Asset Status'}, inplace = True)
asset.rename(columns = {'Root Asset: LMS POS ID' : 'Asset LMS POS ID'}, inplace = True)
asset['Asset LMS POS ID'] = asset['Asset LMS POS ID'].fillna('NA')
asset['Asset LMS POS ID']= asset['Asset LMS POS ID'].astype(str).apply(lambda x: x.replace('.0',''))
asset['Asset LMS POS ID'] = asset['Asset LMS POS ID'].astype(str)
final_2['Plutus TID '] = final_2['Plutus TID '].astype(str)
final_3 = pd.merge(final_2,asset,how='left',left_on='Plutus TID ',right_on='Asset TID')
final_3['Asset LMS POS ID'] = final_3['Asset LMS POS ID'].fillna('NA')
final_3['Asset TID'] = final_3['Asset TID'].fillna('NA')

replacement1.sort_values(by = ['Case: Date/Time Opened'], ascending = False, inplace=True)
replacement = replacement1[['TID','Case Number','Install Date','Type','Status']]
replacement.rename(columns = {'Status' : 'Replacement Status'}, inplace = True)
replacement.rename(columns = {'Type' : 'Replacement Type'}, inplace = True)
replacement['TID'] = replacement['TID'].astype(str)
final_3['Plutus TID ']= final_3['Plutus TID '].astype(str).apply(lambda x: x.replace('.0',''))
final_4 = pd.merge(final_3,replacement,how='left',left_on='Plutus TID ',right_on='TID')
final_4['TID'] = final_4['TID'].fillna('NA')
final_4['Case Number'] = final_4['Case Number'].fillna('NA')
final_4['Install Date'] = final_4['Install Date'].fillna('NA')
final_4['Replacement Type'] = final_4['Replacement Type'].fillna('NA')
final_4['Replacement Status'] = final_4['Replacement Status'].fillna('NA')

deployment1.sort_values(by = ['Created Date/Time'], ascending = False, inplace=True)
deployment = deployment1[['Work Order Line Item: Asset: LMS POS ID','Appointment Number',
                          'Cannot Complete Date','Reschedule Marking Date']]
deployment.rename(columns = {'Work Order Line Item: Asset: LMS POS ID' : 'Deployment LMS POS ID'}, inplace = True)
deployment.rename(columns = {'Appointment Number' : 'Deployment Appointment Number'}, inplace = True)
deployment['Deployment LMS POS ID']= deployment['Deployment LMS POS ID'].astype(str).apply(lambda x: x.replace('.0',''))
deployment['duplicated_flag1'] =deployment['Deployment LMS POS ID'].duplicated() 
deployment = deployment[deployment['duplicated_flag1'] == False]
deployment = deployment.drop(['duplicated_flag1'], axis=1)
deployment['Deployment LMS POS ID'] = deployment['Deployment LMS POS ID'].astype(str)
final_4['Asset LMS POS ID'] = final_4['Asset LMS POS ID'].astype(str)
final_5 = pd.merge(final_4,deployment,how='left',left_on='Asset LMS POS ID',right_on='Deployment LMS POS ID')
final_5['Asset LMS POS ID'] = final_5['Asset LMS POS ID'].astype(str).replace('nan', 'NA')
final_5['Deployment LMS POS ID'] = final_5['Deployment LMS POS ID'].astype(str).replace('nan', 'NA')
final_5['Deployment Appointment Number'] = final_5['Deployment Appointment Number'].fillna('NA')
final_5['Cannot Complete Date'] = final_5['Cannot Complete Date'].fillna('NA')
final_5['Reschedule Marking Date'] = final_5['Reschedule Marking Date'].fillna('NA')

def STPNSTP(c):
     if (c['Cannot Complete Date'] == 'NA' and c['Reschedule Marking Date'] == 'NA'):
         response = 'STP'
     else:
         response = 'NSTP'
     return response
final_5['STP/NSTP'] = final_5.apply(STPNSTP,axis = 1)

deployment1.sort_values(by = ['Created Date/Time'], ascending = False, inplace=True)
deploymentf = deployment1[['Appointment Number','Status','Remarks','Sub Remarks','Actual End','Created Date/Time']]
deploymentf.rename(columns = {'Status' : 'Deployment Status'}, inplace = True)
deploymentf.rename(columns = {'Remarks' : 'Deployment Remarks'}, inplace = True)
deploymentf.rename(columns = {'Sub Remarks' : 'Deployment Sub Remarks'}, inplace = True)
deploymentf.rename(columns = {'Actual End' : 'Deployment Actual End'}, inplace = True)
deploymentf.rename(columns = {'Created Date/Time' : 'Deployment Created Date/Time'}, inplace = True)
deploymentf['Appointment Number'] = deploymentf['Appointment Number'].astype(str)
final_5['Deployment Appointment Number'] = final_5['Deployment Appointment Number'].astype(str)
final_6 = pd.merge(final_5,deploymentf,how='left',left_on='Deployment Appointment Number',right_on='Appointment Number')

deinstall1.sort_values(by = ['Created Date'], ascending = False, inplace=True)
deinstall = deinstall1[['Work Order Line Item: Asset: POS ID','Appointment Number']]
deinstall.rename(columns = {'Appointment Number' : 'De AppointmentNumber'}, inplace = True)
deinstall.rename(columns = {'Work Order Line Item: Asset: POS ID' : 'De POS ID'}, inplace = True)
deinstall['De POS ID']= deinstall['De POS ID'].astype(str).apply(lambda x: x.replace('.0',''))
final_6['Parent POS ID']= final_6['Parent POS ID'].astype(str).apply(lambda x: x.replace('.0',''))
deinstall['duplicated_flag'] =deinstall['De POS ID'].duplicated() 
deinstall = deinstall[deinstall['duplicated_flag'] == False]
deinstall = deinstall.drop(['duplicated_flag'], axis=1)
final_6['Parent POS ID'] = final_6['Parent POS ID'].replace('nan', 'NA')
final_7 = pd.merge(final_6,deinstall,how='left',left_on='Parent POS ID',right_on='De POS ID')

deinstall1.sort_values(by = ['Created Date'], ascending = False, inplace=True)
deinstallf = deinstall1[['Appointment Number','Status','Remarks','Sub Remarks','Actual End','Created Date']]
deinstallf.rename(columns = {'Appointment Number' : 'DeAppointmentNumber'}, inplace = True)
deinstallf.rename(columns = {'Status' : 'De Status'}, inplace = True)
deinstallf.rename(columns = {'Remarks' : 'De Remarks'}, inplace = True)
deinstallf.rename(columns = {'Sub Remarks' : 'De Sub Remarks'}, inplace = True)
deinstallf.rename(columns = {'Actual End' : 'De Actual End'}, inplace = True)
deinstallf.rename(columns = {'Created Date' : 'De Created Date'}, inplace = True)
final_8 = pd.merge(final_7,deinstallf,how='left',left_on='De AppointmentNumber',right_on='DeAppointmentNumber')

def tid_deinstallation(c):
    if (c['De Status'] == 'Completed'):
        response = c['De Actual End']
    else:
        response = '-'
    return response
final_8['De-Installation Date'] = final_8.apply(tid_deinstallation,axis=1)

def problem(c):
    if c['Deployment Status'] == 'Cannot Complete':
        response = c['Cannot Complete Date']
    elif c['De Status'] == 'Cannot Complete':
        response = c['Cannot Complete Date']
    else:
        response = '-'
    return response
final_8['Problematic Date'] = final_8.apply(problem,axis=1)

final_8['Asset TID'] = final_8['Asset TID'].fillna('NA')
final_8['Asset LMS POS ID'] = final_8['Asset LMS POS ID'].astype(str).replace('nan', 'NA')
final_8['Deployment LMS POS ID'] = final_8['Deployment LMS POS ID'].astype(str).replace('nan', 'NA')
final_8['Deployment Appointment Number'] = final_8['Deployment Appointment Number'].astype(str).replace('nan', 'NA')
final_8['Parent POS ID'] = final_8['Parent POS ID'].fillna('NA')
final_8['Appointment Number'] = final_8['Appointment Number'].fillna('NA')
final_8['Deployment Status'] = final_8['Deployment Status'].fillna('NA')
final_8['Deployment Remarks'] = final_8['Deployment Remarks'].fillna('NA')
final_8['Deployment Sub Remarks'] = final_8['Deployment Sub Remarks'].fillna('NA')
final_8['Deployment Actual End'] = final_8['Deployment Actual End'].fillna('NA')
final_8['Deployment Created Date/Time'] = final_8['Deployment Created Date/Time'].fillna('NA')
final_8['De POS ID'] = final_8['De POS ID'].fillna('NA')
final_8['De AppointmentNumber'] = final_8['De AppointmentNumber'].fillna('NA')
final_8['DeAppointmentNumber'] = final_8['DeAppointmentNumber'].fillna('NA')
final_8['De Status'] = final_8['De Status'].fillna('NA')
final_8['De Remarks'] = final_8['De Remarks'].fillna('NA')
final_8['De Sub Remarks'] = final_8['De Sub Remarks'].fillna('NA')
final_8['De Actual End'] = final_8['De Actual End'].fillna('NA')
final_8['De Created Date'] = final_8['De Created Date'].fillna('NA')


final_8.rename(columns={'Asset STATUS':'Asset Status'},inplace=True)
final_8.rename(columns={'De POS ID':'DeInstall POS ID'},inplace=True)
final_8.rename(columns={'De AppointmentNumber':'DeInstall AppointmentNumber'},inplace=True)
final_8.rename(columns={'De Status':'DeInstall Status'},inplace=True)
final_8.rename(columns={'De Remarks':'DeInstall Remarks'},inplace=True)
final_8.rename(columns={'De Sub Remarks':'DeInstall Sub Remarks'},inplace=True)
final_8.rename(columns={'De Actual End':'DeInstall Actual End'},inplace=True)
final_8.rename(columns={'De Created Date':'DeInstall Created Date'},inplace=True)
final_8.rename(columns={'De-Installation Date':'DeInstall Installation Date'},inplace=True)

def status(c):
    if c['Permanent Status'] != 'NA':
        response = c['Permanent Status']
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status,axis=1)

#def status_1(c):
#    if (c['Permanent Status'] == 'NA' and c['Fix Status'] == 'Problematic' and c['Deployment Status'] in ('Re-Schedule','Dispatched','None')):
#        response = 'Planned For Installation'
#    else:
#        response = c['Status']
#    return response
#final_8['Status'] = final_8.apply(status_1,axis=1)

#def status_2(c):
#    if (c['Permanent Status'] == 'NA' and c['Fix Status'] == 'Problematic' and c['Deployment Status'] in ('Cannot Complete')):
#        response = 'Problematic'
#    else:
#        response = c['Status']
#    return response
#final_8['Status'] = final_8.apply(status_2,axis=1)

#def status_3(c):
#    if (c['Permanent Status'] == 'NA' and c['Fix Status'] == 'Problematic' and c['Deployment Status'] in ('Completed') and c['Asset Status'] == 'Pending'):
#        response = 'Planned For Installation'
#    else:
#        response = c['Status']
#    return response
#final_8['Status'] = final_8.apply(status_3,axis=1)

#def status_3(c):
#    if (c['Permanent Status'] == 'NA' and c['Fix Status'] == 'Problematic' and c['Deployment Status'] in ('Completed') and c['Asset Status'] in ('Deactivated','Billing Stop','Live')):
#        response = 'Installed'
#    else:
#        response = c['Status']
#    return response
#final_8['Status'] = final_8.apply(status_3,axis=1)

def status_4(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Installed','De-Installation Under process') and c['DeInstall Status'] in ('RE-SCHEDULE','Dispatched','None')):
        response = 'Planned For De-Installation'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_4,axis=1)

def status_5(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Installed','De-Installation Under process') and c['DeInstall Status'] == 'Cannot Complete'):
        response = 'Problematic- DeInstallation'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_5,axis=1)

def status_6(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Installed','De-Installation Under process') and c['DeInstall Status'] == 'Completed'):
        response = 'De-Installed'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_6,axis=1)

def status_7(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Installed','De-Installation Under process') and c['DeInstall Status'] == 'Asset Cost Recovery'):
        response = 'Problematic- DeInstallation_WDV Reported'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_7,axis=1)

def status_8(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] in ('TID Addition','TID Replacement') and c['Replacement Status'] == 'Closed - Success'):
        response = 'Installed'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_8,axis=1)

def status_9(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] in ('TID Addition','TID Replacement') and c['Replacement Status'] == 'Closed - Problematic' and c['Install Date'] != 'NA'):
        response = 'Installed'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_9,axis=1)

def status_10(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] in ('TID Addition','TID Replacement') and c['Replacement Status'] == 'Closed - Problematic' and c['Install Date'] == 'NA'):
        response = 'Problematic'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_10,axis=1)

def status_11(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] not in ('TID Addition','TID Replacement') and c['Replacement Status'] == 'Closed - Problematic' and c['Install Date'] != 'NA'):
        response = 'Installed'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_11,axis=1)

def status_12(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] not in ('TID Addition','TID Replacement') and c['Replacement Status'] == 'Closed - Problematic' and c['Install Date'] == 'NA'):
        response = 'Planned For Installation'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_12,axis=1)

def status_13(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] == 'NA' and c['Deployment Status'] == 'Completed' and c['Asset Status'] in ('Deactivated','Billing Stop','Live')):
        response = 'Installed'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_13,axis=1)

def status_14(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] == 'NA' and c['Deployment Status'] == 'Completed' and c['Asset Status'] == 'Pending'):
        response = 'Planned For Installation'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_14,axis=1)

def status_15(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] == 'NA' and c['Deployment Status'] == 'Completed' and c['Asset Status'] == 'Cancelled'):
        response = 'Problematic'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_15,axis=1)

def status_16(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] == 'NA' and c['Deployment Status'] in ('Pending for Stock','Re-Schedule','Dispatched','RE-SCHEDULE','None')):
        response = 'Planned For Installation'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_16,axis=1)

def status_17(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] in ('NA','Problematic','Short Closed') and c['DeInstall Status'] == 'NA' and c['Replacement Type'] == 'NA' and c['Deployment Status'] == 'Cannot Complete'):
        response = 'Problematic'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_17,axis=1)

def status_18(c):
    if (c['Permanent Status'] == 'NA' and c['Fix Status'] == 'NA' and c['DeInstall Status'] == 'NA' and c['Replacement Type'] == 'NA' and c['Deployment Status'] == 'NA'):
        response = 'Deployment Not Raised'
    else:
        response = c['Status']
    return response
final_8['Status'] = final_8.apply(status_18,axis=1)

final_8['test_1'] = final_8['Terminal Type'].str.contains(pat = 'Sole')
final_8['test_2'] = final_8['Terminal Type'].str.contains(pat = 'Multi')

def sole_multi(c):
    if c['test_1'] == True:
        response = 'Sole'
    elif c['test_2'] == True:
        response = 'Multi'
    else:
        response = '-'
    return response
final_8['Sole/Multi'] = final_8.apply(sole_multi,axis=1)
final_8 = final_8.drop(['test_1'],axis=1)
final_8 = final_8.drop(['test_2'],axis=1)

final_8.rename(columns={'DeInstall Installation Date':'DeInstallation Date'},inplace=True)
final_8.to_excel('Final Output.xlsx',index=False)

final_8.rename(columns={'Status':'Final Status'},inplace=True)

output_1 = final_8[['ApplicationURN No','Year','IDFC/FD Request Date (DD-MMM-YY)','PL Exisitng Merchant (Yes/No)',
                  'POS ID /Terminal serial number (If PL exisitng merchant)','Region/Zone','ME Legal Name',
                  'ME Commercial Name (DBA)','Installation Postal Address','Installation Pin Code',
                  'Installation City','Installation Landline no.','ME Contact Name','ME Contact Mobile no.',
                  'No. of Plutus TIDs','Pine Labs A/c holder Name','Pine Labs A/c holder email id',
                  'Pine Labs A/c holder Mobile','Plutus TID Released fees per TID (INR)','Asset Status',
                  'TAT for FD Plutus TID release','Business Manager','Business Manager contact number',
                  'Plutus MID Released Date','GSTIN Number','GSTIN state code','Remarks','Plutus MID ',
                  'Plutus TID ','Terminal Type','Account No','IFSC CODE','Final Status',
                  'Sole/Multi','STP/NSTP','DeInstallation Date','Problematic Date','Parent POS ID',
                  'Permanent Status','Permanent TID Installation Date','Permanent De-Installation Date']]

output_1.to_excel('Bank Output.xlsx')
print("job ended at : " + str(datetime.datetime.time(datetime.datetime.now())))


































