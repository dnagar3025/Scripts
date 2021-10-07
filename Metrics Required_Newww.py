#!/usr/bin/env python
# coding: utf-8

# In[1]:



import os
import sys
import pandas as pd
import numpy as np
#import plotly.express as px
import psycopg2
from sqlalchemy import create_engine
import pymssql
#import plotly.graph_objs as go
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import datetime
import json

import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

from pandas import HDFStore, DataFrame, read_csv, concat, options
import warnings
import tables
options.mode.chained_assignment = None
warnings.filterwarnings("ignore", category=tables.NaturalNameWarning)

print('imports completed..')


# In[405]:
conMB = pymssql.connect(host='medibuddy-prod.ccvogfvw9jim.ap-south-1.rds.amazonaws.com',user='deepak.nagar',password='sbNwu7NN4A',database='medimarket')
print('connected to conMB')




# In[2]:


node="mysql+pymysql://analyticswriterole:a1anllytis2345writer123@docsapp-prod-replica.cw68gbfmumcn.ap-south-1.rds.amazonaws.com/node"
connection = create_engine(node)
conRnode = connection.connect()
print('connected to readReplica - node')


# In[3]:


enddate = datetime.date.today()
startdate = datetime.date.today() - datetime.timedelta(days=1)
print(startdate)
print(enddate)


# In[ ]:





# In[4]:


c="""select 'Total_Orders' as tag, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry') """.format(startDate = startdate, endDate = enddate)

CC = pd.read_sql(c, conMB)
CC['Total_Orders'] = CC['Total_Orders'].apply(lambda i :  int(i))
CC


# In[5]:


lll="""select platform, ordertype, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
group by platform, ordertype order by 1,2
""".format(startDate = startdate, endDate = enddate)

CCl = pd.read_sql(lll, conMB)
CCl['Total_Orders'] = CCl['Total_Orders'].apply(lambda i :  int(i))
CCl


# In[6]:


yy="""select Final_Status, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                          else  'Cancelled'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
group by Final_Status
""".format(startDate = startdate, endDate = enddate)


DFF4 = pd.read_sql(yy, conMB)
DFF4


# In[7]:


gf="""select date, Platform, count(distinct R_ID) as orders from (select convert(date,R_CreatedOn) as date,R_ID,  CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform 
          FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
          where 
          R_CreatedOn>='{startDate}' and R_CreatedOn<'{endDate}' and
          R_ContractType='14374' and ISJSON(CONVERT(varchar(max), R_Blob)) > 0  and R_CreatedByUser not like '%test%' and R_EmployeeId not like '%test%' and R_IsActive= 1 and R_EmployeeId != 'Low') X 
          
          group by date,Platform""".format(startDate = startdate, endDate = enddate)


F4 = pd.read_sql(gf, conMB)
F4


# In[8]:


k=CC['Total_Orders'][0]
DFF4['Percent']=DFF4['Total_Orders']*100/k
DFF4['Percent']=DFF4['Percent'].round(decimals=2)
DFF4


# In[9]:


l="""select 'Total_Orders' AS tag, 
       count( RequestID) AS Total_Orders,
      count(distinct RequestID) AS Total_Orders2
FROM rpt.MediMarketRequest (nolock)
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA' """.format(startDate = startdate, endDate = enddate)

CCl = pd.read_sql(l, conMB)
CCl['Total_Orders'] = CCl['Total_Orders'].apply(lambda i :  int(i))
CCl


# In[10]:


Qp = """select Platform, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

  group by platform       """.format(startDate = startdate, endDate = enddate)

dfx = pd.read_sql(Qp, conMB)
dfx


# In[ ]:





# In[11]:


Q4 = """select Platform, OrderType, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN 'Others'
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
and platform like 'Android' and OrderType like 'UPLOADRX'
  group by platform, ordertype  """.format(startDate = startdate, endDate = enddate)

df4 = pd.read_sql(Q4, conMB)
df4


# In[12]:


p1="""select 'Android' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d1 = pd.read_sql(p1, conMB)
d1


# In[ ]:





# In[13]:


Q3 = """select Platform, OrderType, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN 'Others'
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
and platform like 'Android' and OrderType like 'SEARCH'
  group by platform, ordertype """.format(startDate = startdate, endDate = enddate)

df3 = pd.read_sql(Q3, conMB)
df3


# In[14]:


p2="""select 'Android' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d2 = pd.read_sql(p2, conMB)
d2


# In[15]:


Q2 = """ select Platform, OrderType, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN 'Others'
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

and platform Like 'Android' and OrderType Like 'REORDER'
  group by platform, ordertype """.format(startDate = startdate, endDate = enddate)

df2 = pd.read_sql(Q2, conMB)
df2


# In[16]:


p3="""select 'Android' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d3 = pd.read_sql(p3, conMB)
d3


# In[17]:


Q1 = """select Platform, OrderType, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN 'Others'
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN 'ConsultToMeds'
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

and platform like 'Android' and OrderType like 'ConsultToMeds'
  group by platform, ordertype""".format(startDate = startdate, endDate = enddate)

df = pd.read_sql(Q1, conMB)
df


# In[18]:


p4="""select 'Android' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d4 = pd.read_sql(p4, conMB)
d4


# In[19]:


p9="""select 'Web' as Platform, 'UPLOADRX' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d9 = pd.read_sql(p9, conMB)
d9


# In[20]:


Lp= df4.append(d4,ignore_index=True)
Lp= Lp.append(df3,ignore_index=True)
Lp= Lp.append(d2,ignore_index=True)
Lp= Lp.append(df2,ignore_index=True)
Lp= Lp.append(d3,ignore_index=True)
Lp= Lp.append(df,ignore_index=True)
Lp= Lp.append(d4,ignore_index=True)


# In[21]:


Lp


# In[22]:


Q8 = """select Platform, OrderType, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN 'Others'
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN 'ConsultToMeds'
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

and platform like 'Web' and OrderType like 'SEARCH'
  group by platform, ordertype""".format(startDate = startdate, endDate = enddate)

df8 = pd.read_sql(Q8, conMB)
df8


# In[23]:


p5="""select 'Web' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d5 = pd.read_sql(p5, conMB)
d5


# In[24]:


Q7 = """select Platform, OrderType, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN 'Others'
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN 'ConsultToMeds'
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

and platform like 'Web' and OrderType like 'REORDER'
  group by platform, ordertype""".format(startDate = startdate, endDate = enddate)

df7 = pd.read_sql(Q7, conMB)
df7


# In[25]:


p6="""select 'Web' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d6 = pd.read_sql(p6, conMB)
d6


# In[26]:


p10="""select 'Web' as Platform, 'ConsultToMeds' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d10 = pd.read_sql(p10, conMB)
d10


# In[27]:


p11="""select 'Web' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d11 = pd.read_sql(p11, conMB)
d11


# In[28]:


Qs = """select Platform, OrderType, count(distinct RequestID) as Total_Orders from (
select 
    RequestID,R_StatusID,R_Remarks, CASE
                                          WHEN AppointmentStatus = 'Attended' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'On Hold' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Postponed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Settled' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled'
                                          WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Processed' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Received' THEN 'InProcess'
                                          WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered'
                                          WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess'
                                      END AS "Final_Status" ,
      CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN 'Others'
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN 'ConsultToMeds'
              ELSE 'No Info Available'
          END AS OrderType
FROM rpt.MediMarketRequest (nolock)
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
AND isactive=1
LEFT JOIN providermaster.pmr.vwEntityRelation vwer (nolock) ON CorporateERID = vwer.ID
LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
WHERE ContractTypeId IN (10307,
                         10390)
  AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}'   
  and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'
) X
where   R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')

and platform like 'Web' AND OrderType like 'UPLOADRX'
  group by platform, ordertype
         """.format(startDate = startdate, endDate = enddate)

dfy = pd.read_sql(Qs, conMB)
dfy


# In[29]:


p112="""select 'Web' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d112 = pd.read_sql(p112, conMB)
d112


# In[30]:


Lq= dfy.append(d112,ignore_index=True)
Lq= Lq.append(df8,ignore_index=True)
Lq= Lq.append(d5,ignore_index=True)
Lq= Lq.append(df7,ignore_index=True)
Lq= Lq.append(d6,ignore_index=True)
Lq= Lq.append(d10,ignore_index=True)
Lq= Lq.append(d11,ignore_index=True)


# In[31]:


Lq


# In[32]:


p133="""select 'iOS' as Platform, 'UPLOADRX' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d133 = pd.read_sql(p133, conMB)
d133


# In[33]:


p12="""select 'iOS' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d12 = pd.read_sql(p12, conMB)
d12


# In[34]:


Q6 = """ SELECT 
       Platform,
       OrderType,
       count(RequestID) AS Total_Orders
       
FROM
  (SELECT CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType,
          RequestId,
          CASE
              WHEN AppointmentStatusId IN (57,
                                           87) THEN 1
          END AS Delivered,
          convert(date, ApptCreatedDate) AS ApptCreatedDate
   FROM rpt.MediMarketRequest (nolock) mmr
   LEFT JOIN dbo.tblRequest (nolock) tbr ON mmr.RequestId = tbr.R_Id
   LEFT JOIN dbo.tblOrder (nolock) tbo ON mmr.OrderId = tbo.O_Id
   WHERE ApptCreatedDate >='{startDate}' AND ApptCreatedDate< '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)) A
                            where platform like 'iOS' and OrderType like 'SEARCH'
GROUP BY ApptCreatedDate,
         Platform,
         OrderType
ORDER BY 1,
         2,
         3""".format(startDate = startdate, endDate = enddate)

df6 = pd.read_sql(Q6, conMB)
df6


# In[35]:


p7="""select 'iOS' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d7 = pd.read_sql(p7, conMB)
d7


# In[36]:


Q5 = """ SELECT 
       Platform,
       OrderType,
       count(RequestID) AS Total_Orders
       
FROM
  (SELECT CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType,
          RequestId,
          CASE
              WHEN AppointmentStatusId IN (57,
                                           87) THEN 1
          END AS Delivered,
          convert(date, ApptCreatedDate) AS ApptCreatedDate
   FROM rpt.MediMarketRequest (nolock) mmr
   LEFT JOIN dbo.tblRequest (nolock) tbr ON mmr.RequestId = tbr.R_Id
   LEFT JOIN dbo.tblOrder (nolock) tbo ON mmr.OrderId = tbo.O_Id
   WHERE ApptCreatedDate >='{startDate}' AND ApptCreatedDate< '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)) A
                            where platform like 'iOS' and OrderType like 'REORDER'
GROUP BY ApptCreatedDate,
         Platform,
         OrderType
ORDER BY 1,
         2,
         3""".format(startDate = startdate, endDate = enddate)

df5 = pd.read_sql(Q5, conMB)
df5


# In[37]:


Qo = """ SELECT 
       Platform,
    
       count(distinct RequestID) AS Total_Orders
       
FROM
  (SELECT CASE
              WHEN Properties LIKE '%source\":\"web%' THEN 'Web'
              WHEN Properties LIKE '%source\":\"AndroidMB%' THEN 'Android'
              WHEN Properties LIKE '%source\":\"iOSMB%' THEN 'iOS'
              WHEN lower(O_Blob) LIKE '%"createdByApp"%' THEN JSON_VALUE(CONVERT(varchar(MAX), O_Blob), '$.createdByApp')
              ELSE 'No Info Available'
          END AS Platform,
          CASE
              WHEN lower(Properties) Like '%%type\\":\\"search%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"uploadrx%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%type\\":\\"onlyotc%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"reorder%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%%type\\":\\"re-order%%' THEN 'REORDER'
              WHEN lower(Properties) LIKE '%type\\":\\"essentials%%' THEN 'SEARCH'
              WHEN lower(Properties) LIKE '%%type\\":\\"prescription%%' THEN 'UPLOADRX'
              WHEN lower(Properties) LIKE '%%"BookingSource"%%' THEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.BookingSource')
              ELSE 'No Info Available'
          END AS OrderType,
          RequestId, employeeid,
          CASE
              WHEN AppointmentStatusId IN (57,
                                           87) THEN 1
          END AS Delivered,
          convert(date, ApptCreatedDate) AS ApptCreatedDate
   FROM rpt.MediMarketRequest (nolock) mmr
   LEFT JOIN dbo.tblRequest (nolock) tbr ON mmr.RequestId = tbr.R_Id
   LEFT JOIN dbo.tblOrder (nolock) tbo ON mmr.OrderId = tbo.O_Id
   WHERE ApptCreatedDate >='{startDate}' AND ApptCreatedDate< '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)) A
     where EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'                       
GROUP BY ApptCreatedDate,
         Platform
        
ORDER BY 1,
         2
         """.format(startDate = startdate, endDate = enddate)

dfo = pd.read_sql(Qo, conMB)
dfo


# In[38]:


p8="""select 'Web' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d8 = pd.read_sql(p8, conMB)
d8


# In[39]:


p13="""select 'iOS' as Platform, 'ConsultToMeds' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d13 = pd.read_sql(p13, conMB)
d13


# In[40]:


p14="""select 'iOS' as Platform, '% Conversion' as OrderType, '-------' as Total_Orders """.format(startDate = startdate, endDate = enddate)

d14 = pd.read_sql(p14, conMB)
d14


# In[41]:


Lr= d133.append(d12,ignore_index=True)
Lr= Lr.append(df6,ignore_index=True)
Lr= Lr.append(d7,ignore_index=True)
Lr= Lr.append(df5,ignore_index=True)
Lr= Lr.append(d8,ignore_index=True)
Lr= Lr.append(d13,ignore_index=True)
Lr= Lr.append(d14,ignore_index=True)


# In[42]:


Lr


# In[43]:


a="""select 'Total_GMV' AS tag, sum(OOP_GMV+Corp_GMV) as GMV_Sum from (SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0)
                AND (CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited')
                     OR MAWallet >0
                     OR CorpWallet >0) THEN 'OOP with Wallet'
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0) THEN 'OOP with NoWallet'
           WHEN PMEntityId = '1018900' THEN 'Pure Retail'
           ELSE 'Corporate'
       END AS Flag,
       PaymentReceivedByPG + Cash + MBWallet AS OOP_Ordervalue,
       MAWallet + CorpWallet AS Corp_Ordervalue,
       TotalAmount*(PaymentReceivedByPG + Cash + MBWallet)/NetPaidAmount AS OOP_GMV,
                                                           TotalAmount*(MAWallet + CorpWallet)/NetPaidAmount AS Corp_GMV
FROM
  (SELECT DISTINCT RequestId,
                   JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId,
                   CorporateName,
                   AppointmentStatusId,
                   AppointmentStatus,
                   CASE
                       WHEN rf.RF_Id IS NOT NULL THEN convert(date, rf.RF_CreatedOn)
                   END AS DeliveryTime0
   FROM MediMarket.rpt.MediMarketRequest (nolock)
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_Id ,
             RF_IsActive,
             RF_StatusId,
             RF_CreatedOn
      FROM MediMarket..tblRequestFlow(nolock)
      WHERE RF_CreatedOn >='{startDate}'
        AND RF_CreatedOn < '{endDate}') rf ON RequestId = rf.RF_R_ID
   AND RF_IsActive = 1
   AND RF_StatusId IN (86,
                       87,
                       57)
   WHERE rf.RF_CreatedOn >='{startDate}'
     AND rf.RF_CreatedOn < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)
     AND (CorporateName != 'MA Board'
          OR CorporateName IS NULL)
     AND (EmployeeID != 'Non_Board_DA'
          OR EmployeeID IS NULL)) mmr
LEFT JOIN
  (SELECT PI_R_Id,
          sum(PI_quantity*PI_unitPrice) AS TotalAmount,
          sum(PI_discountAmount) AS TotalDiscount,
          sum(PI_quantity*PI_unitPrice) - sum(PI_discountAmount) AS DiscountedAmount,
          sum(PI_cashLessComponent + PI_maComponent + PI_cashComponent) AS NetPaidAmount,
          sum(CASE WHEN WalletName LIKE 'MAWallet' THEN PI_cashLessComponent ELSE 0 END) AS MAWallet,
          sum(CASE WHEN WalletName LIKE 'CorpWallet' THEN PI_cashLessComponent ELSE 0 END) AS CorpWallet,
          sum(PI_maComponent) AS MBWallet,
          sum(CASE WHEN WalletName LIKE 'PaymentReceivedByPG' THEN PI_cashLessComponent ELSE 0 END) AS PaymentReceivedByPG,
          sum(PI_cashComponent) AS Cash
   FROM
     (SELECT PI_id,
             PI_R_Id,
             PI_quantity,
             PI_unitPrice,
             PI_discountAmount,
             PI_cashLessComponent,
             PI_maComponent,
             PI_cashComponent,
             JSON_VALUE(CONVERT(varchar(MAX), PI_walletBlob), '$[0].WalletName') AS WalletName
      FROM MediMarket.Provider.PharmacyItem (nolock)) x
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id) D group by convert(date,DeliveryTime0) """.format(startDate = startdate, endDate = enddate)

AA = pd.read_sql(a, conMB)
AA['GMV_Sum'] = AA['GMV_Sum'].apply(lambda i :  int(i))
AA


# In[44]:


B=""" SELECT  'Out of pocket GMV' AS tag, sum(OOP_GMV) as GMV_Sum

FROM (SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0 or MBWallet>0)
                AND ((PMEntityId in (1006639,1006191,1020364,1006621,1006205,1006772,1070558,1159401,1006205,1006622,1046131,1032702,1020343,1006637,1041377,106993,1006206,11172035,1163818,1163817,1006438,1027303,1027841,1068085, 1022478,1163816,1006772,1045855,11172519,11172549,10232,1163918)
                or CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited'))
                    and (MAWallet>0 or CorpWallet>0)
                      )THEN 'OOP with Wallet'
                     
                      WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0 or MBWallet>0)
                AND (CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited')
                      )THEN 'OOP with NoWallet'
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0 or MBWallet>0) THEN 'Retail Corp'
           WHEN PMEntityId = '1018900' THEN 'Pure Retail'
           ELSE 'Corporate'
       END AS Flag,
       PaymentReceivedByPG + Cash + MBWallet AS OOP_Ordervalue,
       MAWallet + CorpWallet AS Corp_Ordervalue,
       TotalAmount*(PaymentReceivedByPG + Cash + MBWallet)/NetPaidAmount AS OOP_GMV,
                                                           TotalAmount*(MAWallet + CorpWallet)/NetPaidAmount AS Corp_GMV
FROM
  (SELECT DISTINCT RequestId,
                   JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId,
                   CorporateName,
                   AppointmentStatusId,
                   AppointmentStatus,
                   CASE
                       WHEN rf.RF_Id IS NOT NULL THEN convert(date, rf.RF_CreatedOn)
                   END AS DeliveryTime0
   FROM MediMarket.rpt.MediMarketRequest (nolock)
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_Id ,
             RF_IsActive,
             RF_StatusId,
             RF_CreatedOn
      FROM MediMarket..tblRequestFlow(nolock)
      WHERE RF_CreatedOn >='{startDate}'
        AND RF_CreatedOn < '{endDate}') rf ON RequestId = rf.RF_R_ID
   AND RF_IsActive = 1
   AND RF_StatusId IN (86,
                       87,
                       57)
   WHERE rf.RF_CreatedOn >='{startDate}'
     AND rf.RF_CreatedOn < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)
     AND (CorporateName != 'MA Board'
          OR CorporateName IS NULL)
     AND (EmployeeID != 'Non_Board_DA'
          OR EmployeeID IS NULL)) mmr
LEFT JOIN
  (SELECT PI_R_Id,
          sum(PI_quantity*PI_unitPrice) AS TotalAmount,
          sum(PI_discountAmount) AS TotalDiscount,
          sum(PI_quantity*PI_unitPrice) - sum(PI_discountAmount) AS DiscountedAmount,
          sum(PI_cashLessComponent + PI_maComponent + PI_cashComponent) AS NetPaidAmount,
          sum(CASE WHEN WalletName LIKE 'MAWallet' THEN PI_cashLessComponent ELSE 0 END) AS MAWallet,
          sum(CASE WHEN WalletName LIKE 'CorpWallet' THEN PI_cashLessComponent ELSE 0 END) AS CorpWallet,
          sum(PI_maComponent) AS MBWallet,
          sum(CASE WHEN WalletName LIKE 'PaymentReceivedByPG' THEN PI_cashLessComponent ELSE 0 END) AS PaymentReceivedByPG,
          sum(PI_cashComponent) AS Cash
   FROM
     (SELECT PI_id,
             PI_R_Id,
             PI_quantity,
             PI_unitPrice,
             PI_discountAmount,
             PI_cashLessComponent,
             PI_maComponent,
             PI_cashComponent,
             JSON_VALUE(CONVERT(varchar(MAX), PI_walletBlob), '$[0].WalletName') AS WalletName
      FROM MediMarket.Provider.PharmacyItem (nolock)) x
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id) D  where Flag IN ('OOP with NoWallet', 'OOP with Wallet') group by convert(date,DeliveryTime0)
""".format(startDate = startdate, endDate = enddate)

bb = pd.read_sql(B, conMB)
bb['GMV_Sum'] = bb['GMV_Sum'].apply(lambda i :  int(i))
bb


# In[45]:


b="""select 'AOV' AS tag, sum(OOP_GMV+Corp_GMV)/count(distinct RequestID) as GMV_Sum FROM (SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0)
                AND (CorporateName IN ('Tata Consultancy Services Ltd',
                                       'Intel Technology India Pvt Ltd',
                                       'Tata Motors Limited',
                                       'Synopsys India Pvt Ltd-1',
                                       'Medi Buddy',
                                       'J P Morgan Services India Pvt Ltd',
                                       'WM Global Technology Services Pvt Ltd',
                                       'Ovhtech R&D (India) Private Ltd',
                                       'Synamedia Technologies(INDIA) Pvt Ltd',
                                       'J P Morgan Chase Bank',
                                       'Synopsys India Pvt Ltd-2',
                                       'Synopsys India Pvt Ltd',
                                       'Goproducts Engineering India LLP',
                                       'Synopsys (India) Pvt Ltd',
                                       'Tata Consulting Engineers Limited',
                                       'Indian Institute Of Management Bangalore',
                                       'Katerra India Private Limited',
                                       'J P Morgan India Pvt Limited',
                                       'Rakuten India Enterprise Pvt Ltd',
                                       'One.In Digitech Media Private Limited',
                                       'DocsApp',
                                       'Medi Assist',
                                       'Greynium Information Technologies Pvt Ltd',
                                       'Medibuddy',
                                       'NTT DATA',
                                       'Infoblox Technical Support & SWDVPT Pvt Ltd',
                                       'Bharti Airtel Limited & Its Subsidiaries/ Associate',
                                       'Quess Corp Ltd',
                                       'Verse Innovation Private Limited',
                                       'WM Global Technology Services Pvt Ltd-1',
                                       'Tata Chemicals Ltd-1',
                                       'Lowe%s Services India Pvt Ltd',
                                       'Hyperverge',
                                       'Intuit',
                                       'WM GLOBAL Sourcing India Private Limited')
                     OR MAWallet >0
                     OR CorpWallet >0) THEN 'OOP with Wallet'
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0) THEN 'OOP with NoWallet'
           WHEN PMEntityId = '1018900' THEN 'Pure Retail'
           ELSE 'Corporate'
       END AS Flag,
       PaymentReceivedByPG + Cash + MBWallet AS OOP_Ordervalue,
       MAWallet + CorpWallet AS Corp_Ordervalue,
       TotalAmount*(PaymentReceivedByPG + Cash + MBWallet)/NetPaidAmount AS OOP_GMV,
                                                           TotalAmount*(MAWallet + CorpWallet)/NetPaidAmount AS Corp_GMV
FROM
  (SELECT DISTINCT RequestId,
                   JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId,
                   CorporateName,
                   AppointmentStatusId,
                   AppointmentStatus,
                   CASE
                       WHEN rf.RF_Id IS NOT NULL THEN convert(date, rf.RF_CreatedOn)
                   END AS DeliveryTime0
   FROM MediMarket.rpt.MediMarketRequest (nolock)
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_Id ,
             RF_IsActive,
             RF_StatusId,
             RF_CreatedOn
      FROM MediMarket..tblRequestFlow(nolock)
      WHERE RF_CreatedOn >='{startDate}'
        AND RF_CreatedOn < '{endDate}') rf ON RequestId = rf.RF_R_ID
   AND RF_IsActive = 1
   AND RF_StatusId IN (86,
                       87,
                       57)
   WHERE rf.RF_CreatedOn >='{startDate}'
     AND rf.RF_CreatedOn < '{endDate}'
     AND ContractTypeId IN (10307,
                            10390)
     AND (CorporateName != 'MA Board'
          OR CorporateName IS NULL)
     AND (EmployeeID != 'Non_Board_DA'
          OR EmployeeID IS NULL)) mmr
LEFT JOIN
  (SELECT PI_R_Id,
          sum(PI_quantity*PI_unitPrice) AS TotalAmount,
          sum(PI_discountAmount) AS TotalDiscount,
          sum(PI_quantity*PI_unitPrice) - sum(PI_discountAmount) AS DiscountedAmount,
          sum(PI_cashLessComponent + PI_maComponent + PI_cashComponent) AS NetPaidAmount,
          sum(CASE WHEN WalletName LIKE 'MAWallet' THEN PI_cashLessComponent ELSE 0 END) AS MAWallet,
          sum(CASE WHEN WalletName LIKE 'CorpWallet' THEN PI_cashLessComponent ELSE 0 END) AS CorpWallet,
          sum(PI_maComponent) AS MBWallet,
          sum(CASE WHEN WalletName LIKE 'PaymentReceivedByPG' THEN PI_cashLessComponent ELSE 0 END) AS PaymentReceivedByPG,
          sum(PI_cashComponent) AS Cash
   FROM
     (SELECT PI_id,
             PI_R_Id,
             PI_quantity,
             PI_unitPrice,
             PI_discountAmount,
             PI_cashLessComponent,
             PI_maComponent,
             PI_cashComponent,
             JSON_VALUE(CONVERT(varchar(MAX), PI_walletBlob), '$[0].WalletName') AS WalletName
      FROM MediMarket.Provider.PharmacyItem (nolock)) x
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id)D group by convert(date,DeliveryTime0)""".format(startDate = startdate, endDate = enddate)

BB = pd.read_sql(b, conMB)
BB['GMV_Sum'] = BB['GMV_Sum'].apply(lambda i :  int(i))
BB


# In[46]:


Ly= AA.append(bb,ignore_index=True)
Ly= Ly.append(BB,ignore_index=True)


# In[47]:


Ly


# In[48]:


p=Ly['GMV_Sum'].where(Ly['tag']=='Out of pocket GMV').dropna()[1]
k=Ly['GMV_Sum'].where(Ly['tag']=='Total_GMV').dropna()[0]


# In[49]:


vv="""select 'OOP Percentage' as tag"""
vp = pd.read_sql(vv, conMB)

vp


# In[50]:


d=p*100/k


# In[51]:


vp['OOP Percent']=d


# In[52]:


vp['OOP Percent']=vp['OOP Percent'].round(2)


# In[53]:


vp


# In[54]:


p/k


# In[55]:


p


# In[ ]:





# In[ ]:





# In[56]:


k


# In[57]:


o="""SELECT 
round(avg(CASE WHEN OverallRating IS NOT NULL THEN convert(float,OverallRating) ELSE 0 END),1) AS avg_rating,
       count(DISTINCT id) AS No_of_Responses
       
FROM MediMarket..tblFeedbackReport(NOLOCK)
WHERE Category = 'Meds' and OverallRating !=0
  AND CREATEDON>= '{startDate}'
  AND CREATEDON < '{endDate}'
GROUP BY convert(date, CREATEDON)""".format(startDate = startdate, endDate = enddate)
pp = pd.read_sql(o, conMB)
pp


# In[ ]:





# In[58]:


ee="""select  'Active MB Gold users' as tag ,count(distinct A.patientid) as count_ from Subscription  A left join Consults B on B.patient=A.patientid
left join MedibuddyDocsappMappings C on C.patientid= B.patient
where A.validtill>='{startDate}' and medibuddyuserid is not null""".format(startDate = startdate, endDate = enddate)
DFF1 = pd.read_sql(ee, node)
DFF1


# In[59]:


rr="""SELECT TAU_ID,REQUESTID,CONTACTNO FROM  MediAuth2..tblapplicationuser tau WITH(nolock) 
INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tau.tau_id=R_createdby
INNER JOIN rpt.MediMarketRequest (nolock) 
ON tr.R_Id=RequestID
WHERE EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA' 
  AND ApptCreatedDate>='{startDate}'
     AND ApptCreatedDate < '{endDate}'
      AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
   AND    contractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)""".format(startDate = startdate, endDate = enddate)
DFF2 = pd.read_sql(rr, conMB)
DFF2


# In[60]:


tt="""select   A.patientid,medibuddyuserid from Subscription A left join Consults B on B.patient=A.patientid
left join MedibuddyDocsappMappings C on C.patientid= B.patient
where A.validtill>='{startDate}' and medibuddyuserid is not null""".format(startDate = startdate, endDate = enddate)
DFF3 = pd.read_sql(tt, node)
DFF3


# In[61]:


dff=DFF2.merge(DFF3, how='inner',  left_on='TAU_ID', right_on='medibuddyuserid')


# In[62]:


dff['REQUESTID'].nunique()


# In[63]:


data = [['Medicine Orders (Gold Users)', dff['REQUESTID'].nunique()]]


# In[64]:


dfff=pd.DataFrame(data,columns= ['tag','count_'])


# In[65]:


dfff


# In[66]:


data1 = [['Unique Users (Gold Users )', dff['CONTACTNO'].nunique()]]


# In[67]:


dfff1=pd.DataFrame(data1,columns= ['tag','count_'])


# In[68]:


dfff1


# In[69]:


Lx= DFF1.append(dfff,ignore_index=True)
Lx= Lx.append(dfff1,ignore_index=True)


# In[70]:


Lx


# In[71]:


k=100*dfff['count_']/DFF1['count_']


# In[72]:


p=k[0]


# In[73]:


p


# In[74]:


207*100/62751


# In[ ]:





# In[75]:


sample data : tag:G2M
fromDate:
key:G2M
value:


# In[76]:



import requests
url = 'http://dataengineering.medibuddy.in:8888/metrics/ingest'
data_={
    "tag": 'G2M',
    "key": "G2M",
    "value": p,
    "fromDate": datetime.datetime.now().date()}

x = requests.post(url, data = data_)

print(x.text)


# In[ ]:





# In[77]:


HEADER = """<html>
<head>
<style type="text/css">


.dataframe {
  font-family: "Open Sans", "HelveticaNeue", "Helvetica Neue", Helvetica, Arial, sans-serif;
  border-collapse: collapse;
  width: 90%;
}
.dataframe td, .dataframe th {
  border: 1px solid #4C4C4C;
  padding: 4px;
}

.dataframe tr:nth-child(even){background-color: #f2f2f2;}

.dataframe tr:hover {background-color: #add8e6;}

.dataframe th {
  padding-top: 2px;
  padding-bottom: 2px;
  text-align: center;
  background-color: #add8e6;
  color: #000000;
}

.dataframe td {
  text-align: center;
}

</style>

"""

FOOTER = """</body></html>"""

print('Imported Header and Footer...')

# In[407]:

gmail_user  = "deepak.nagar@medibuddy.in"
gmail_pwd="nmoeawovxqreawmj"


# In[437]:


def mail(to, subject, text, attach=None):

   msg = MIMEMultipart()
   msg['From'] = gmail_user
   msg['To'] = ", ".join(to)
   msg['Subject'] = subject

   msg.attach(MIMEText(text, 'html'))
   if attach:
       for items in attach:
              part = MIMEBase('application', 'octet-stream')
              part.set_payload(open(items, 'rb').read())
              Encoders.encode_base64(part)
              part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(items))
              msg.attach(part)
   mailServer = smtplib.SMTP("smtp.gmail.com", 587)
   mailServer.ehlo()
   mailServer.starttls()
   mailServer.ehlo()
   mailServer.login(gmail_user, gmail_pwd)
   mailServer.sendmail(gmail_user, to, msg.as_string())
   mailServer.close()


# In[78]:


html_CC= CC.to_html(justify ='center', index=False).replace('\n','')
html_vp= vp.to_html(justify ='center', index=False).replace('\n','')
html_df= df.to_html(justify ='center', index=False).replace('\n','')
html_Ly= Ly.to_html(justify ='center', index=False).replace('\n','')
html_Lp= Lp.to_html(justify ='center', index=False).replace('\n','')
html_Lq= Lq.to_html(justify ='center', index=False).replace('\n','')
html_Lr= Lr.to_html(justify ='center', index=False).replace('\n','')
html_pp= pp.to_html(justify ='center', index=False).replace('\n','')
html_dfx= dfx.to_html(justify ='center', index=False).replace('\n','')
html_Lx= Lx.to_html(justify ='center', index=False).replace('\n','')
html_DFF4= DFF4.to_html(justify ='center', index=False).replace('\n','')


# In[79]:


html = "<h2> Total Orders  </h2>" + html_CC + '<br/><br/>' + "<h2>Total order split </h2>" + html_DFF4  + "<h2> Order Split (Platform Wise) </h2>" + html_dfx +"<h2> Android Orders </h2>" + html_Lp + '<br/><br/>' + "<h2> Web Orders  </h2>" + html_Lq + '<br/><br/>'  +  "<h2> GMV and AOV  </h2>" + html_Ly + '<br/><br/>'   +  "<h2> Out of pocket percent  </h2>" + html_vp + '<br/><br/>' +  "<h2> CSAT </h2>" + html_pp  + "<h2> Gold to Meds  </h2>" + html_Lx + '<br/><br/>' 

today = datetime.date.today()
yesterday = (today - datetime.timedelta(days=1)).strftime('%d-%b-%Y')
print(yesterday)
    
Heading = 'MECE' + '(' + yesterday + ')'
recipients = ["deepak.nagar@medibuddy.in","sivathanu.k@docsapp.in","adhithya.parthasarathi@medibuddy.in","sk@medibuddy.in","sruteesh.kumar@medibuddy.in","Chaitanya@medibuddy.in"]

#recipients = ["mukul.bansal@docsapp.in","ashima.setia@medibuddy.in","yashaswee.p@medibuddy.in","srishti.jain@docsapp.in","avinash.kumar@docsapp.in"]


#recipients = ["mukul.bansal@docsapp.in","avinash.kumar@docsapp.in","sonal@docsapp.in","keerthi.kiran@medibuddy.in",
#"shreyas@docsapp.in","balakrishna@docsapp.in","rahul.bora@medibuddy.in","harsha@medibuddy.in","yashaswee.p@medibuddy.in",
#"prachi.singh@medibuddy.in","sandeep.singh@medibuddy.in","srishti.jain@docsapp.in"]

email_body = HEADER + html + FOOTER
#print(email_body)
mail(recipients,Heading,email_body,None)
print('mail sent')
    



conMB.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[153]:


S


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


dd="""select  A.patientid, medibuddyuserid from Subscription A left join Consults B on B.patient=A.patientid 
left join MedibuddyDocsappMappings C on C.patientid= B.patient
where A.validTill>='{startDate}' and medibuddyuserid is not null""".format(startDate = startdate, endDate = enddate)
CC = pd.read_sql(dd, node)

CC


# In[ ]:


kk="""select  requestid, tau_id,contactno from MediAuth2..tblapplicationuser tau WITH(nolock) inner join MediMarket.dbo.tblrequest tr WITH (nolock)ON tau.tau_id=R_createdby
inner join rpt.MediMarketRequest (nolock) on requestid=tr.R_Id

where ApptCreatedDate>='{startDate}' and ApptCreatedDate<'{endDate}'
and ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order.' , 'Duplicate Order' , 'Duplicate Entry')
and EmployeeID IS not NULL
  AND EmployeeID <> 'Non_Board_DA'""".format(startDate = startdate, endDate = enddate)

kk = pd.read_sql(kk, conMB)

kk


# In[ ]:


dff=kk.merge(CC, how='inner',  left_on='tau_id', right_on='medibuddyuserid')


# In[ ]:


dff['contactno'].nunique()


# In[ ]:


po="""select count(distinct TAU_PHONENUMBER) from (select  tau_id,TAU_FIRSTNAME, TAU_LASTNAME,TAU_LOGINNAME,TAU_PASSWORD ,TAU_PROVIDERMASTERENTITYID,TAU_EMAILID,TAU_PHONENUMBER,TAU_CREATEDBY,TAU_CREATEDON,MODIFIEDON,TAU_HASLOGGEDIN,TAU_ACCOUNTLOCKEDON,TAU_MODIFIEDBY,  JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.additionals.PMEntityId') AS PMEntityId from MediAuth2..tblapplicationuser tau WITH(nolock) inner join MediMarket.dbo.tblrequest tr WITH (nolock)ON tau.tau_id=R_createdby
inner join rpt.MediMarketRequest (nolock) on requestid=tr.R_Id

where ApptCreatedDate >='{startDate}' AND ApptCreatedDate< '{endDate}'

AND R_Remarks not in ('Doctor consultation successful, order will be processed again', 'Reason : Placed a new order;Placed a new order', 'Due to technical issue order got cancelled and new order raised with correct address.', 'We are unable to proceed your order due to the technical and hence we have placed the new order' , 'Duplicate Order' , 'Duplicate Entry')) Z where PMEntityId=1006639 """.format(startDate = startdate, endDate = enddate)

hh = pd.read_sql(po, conMB)

hh


# In[ ]:


hh.to_csv('Tcs_Auth.csv', index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




