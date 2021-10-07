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


enddate = datetime.date.today()
startdate = datetime.date.today() - datetime.timedelta(days=7)
enddate1 = datetime.date.today()- datetime.timedelta(days=7)
startdate1 = datetime.date.today() - datetime.timedelta(days=14)
print(startdate)
print(enddate)
print(startdate1)
print(enddate1)


# In[3]:


Q = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'

 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'


) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' """.format(startDate = startdate, endDate = enddate)

df = pd.read_sql(Q, conMB)
df


# In[4]:


d1=df['EVENT'].value_counts().head(5)


# In[5]:


d1=d1.to_frame()


# In[6]:


A = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'

 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'


) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' """.format(startDate = startdate1, endDate = enddate1)

df_1 = pd.read_sql(A, conMB)
df_1


# In[7]:


d2=df_1['EVENT'].value_counts().head(5)


# In[8]:


d2=d2.to_frame()


# In[9]:


df['EVENT'].value_counts()


# In[10]:


df_1['EVENT'].value_counts()


# In[11]:


p1=df['TAT'].where(df['EVENT']=='Confirmed by Retailer_Placed with Partner')
p1.quantile(0.5)
p1.quantile(0.6)
p1.quantile(0.7)
p1.quantile(0.8)
p1.quantile(0.9)
p1.quantile(0.95)
p1.quantile(0.99)
p1.quantile(1.0)


# In[12]:


p_1=df_1['TAT'].where(df_1['EVENT']=='Confirmed by Retailer_Placed with Partner')


# In[13]:


p1.quantile(0.5)


# In[14]:


p1.quantile(0.6)


# In[15]:


p1.quantile(0.7)


# In[16]:


p1.quantile(0.8)


# In[17]:


p1.quantile(0.9)


# In[18]:


p1.quantile(0.95)


# In[19]:


p1.quantile(0.99)


# In[20]:


p1.quantile(1.0)


# In[21]:


p_1.quantile(0.5)


# In[22]:


p_1.quantile(0.6)


# In[23]:


p_1.quantile(0.7)


# In[24]:


p_1.quantile(0.8)


# In[25]:


p_1.quantile(0.9)


# In[26]:


p_1.quantile(0.95)


# In[27]:


p_1.quantile(0.99)


# In[28]:


p_1.quantile(1.0)


# In[ ]:





# In[29]:


p2=df['TAT'].where(df['EVENT']=='Invoice Pending_Confirmed by Retailer')
p2.quantile(0.5)


# In[30]:


p_2=df_1['TAT'].where(df_1['EVENT']=='Invoice Pending_Confirmed by Retailer')
p_2.quantile(0.5)


# In[31]:


p_2.quantile(0.6)


# In[32]:


p_2.quantile(0.7)


# In[33]:


p_2.quantile(0.8)


# In[34]:


p_2.quantile(0.9)


# In[35]:


p_2.quantile(0.95)


# In[36]:


p_2.quantile(0.99)


# In[37]:


p_2.quantile(1.0)


# In[ ]:





# In[38]:


p2.quantile(0.6)


# In[39]:


p2.quantile(0.7)


# In[40]:


p2.quantile(0.8)


# In[41]:


p2.quantile(0.9)


# In[42]:


p2.quantile(0.95)


# In[43]:


p2.quantile(0.99)


# In[44]:


p2.quantile(1.0)


# In[45]:


p3=df['TAT'].where(df['EVENT']=='Invoice Generated_Invoice Pending')
p3.quantile(0.5)


# In[46]:


p_3=df_1['TAT'].where(df_1['EVENT']=='Invoice Generated_Invoice Pending')
p_3.quantile(0.5)


# In[47]:


p_3.quantile(0.6)


# In[48]:


p_3.quantile(0.7)


# In[49]:


p_3.quantile(0.8)


# In[50]:


p_3.quantile(0.9)


# In[51]:


p_3.quantile(0.95)


# In[52]:


p_3.quantile(0.99)


# In[53]:


p_3.quantile(1.0)


# In[ ]:





# In[ ]:





# In[54]:


p3.quantile(0.6)


# In[55]:


p3.quantile(0.7)


# In[56]:


p3.quantile(0.8)


# In[57]:


p3.quantile(0.9)


# In[58]:


p3.quantile(0.95)


# In[59]:


p3.quantile(0.99)


# In[60]:


p3.quantile(1.0)


# In[ ]:





# In[61]:


p4=df['TAT'].where(df['EVENT']=='Out for Delivery_Invoice Generated')
p4.quantile(0.5)


# In[62]:


p_4=df_1['TAT'].where(df_1['EVENT']=='Out for Delivery_Invoice Generated')
p_4.quantile(0.5)


# In[63]:


p_4.quantile(0.6)


# In[64]:


p_4.quantile(0.7)


# In[65]:


p_4.quantile(0.8)


# In[66]:


p_4.quantile(0.9)


# In[67]:


p_4.quantile(0.95)


# In[68]:


p_4.quantile(0.99)


# In[69]:


p_4.quantile(1.0)


# In[70]:


p4.quantile(0.6)


# In[71]:


p4.quantile(0.7)


# In[72]:


p4.quantile(0.8)


# In[73]:


p4.quantile(0.9)


# In[74]:


p4.quantile(0.95)


# In[75]:


p4.quantile(0.99)


# In[76]:


p4.quantile(1.0)


# In[ ]:





# In[77]:


p5=df['TAT'].where(df['EVENT']=='Delivered_Out for Delivery')
p5.quantile(0.5)


# In[78]:


p_5=df_1['TAT'].where(df_1['EVENT']=='Delivered_Out for Delivery')
p_5.quantile(0.5)


# In[79]:


p_5.quantile(0.6)


# In[80]:


p_5.quantile(0.7)


# In[81]:


p_5.quantile(0.8)


# In[82]:


p_5.quantile(0.9)


# In[83]:


p_5.quantile(0.95)


# In[84]:


p_5.quantile(0.99)


# In[85]:


p_5.quantile(1.0)


# In[ ]:





# In[86]:


p5.quantile(0.6)


# In[87]:


p5.quantile(0.7)


# In[88]:


p5.quantile(0.8)


# In[89]:


p5.quantile(0.9)


# In[90]:


p5.quantile(0.95)


# In[91]:


p5.quantile(0.99)


# In[92]:


p5.quantile(1.0)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[93]:


R = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (115,87)
 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (115,87)

) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' """.format(startDate = startdate, endDate = enddate)

df1 = pd.read_sql(R, conMB)
df1


# In[94]:


B = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (115,87)
 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (115,87)

) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' """.format(startDate = startdate1, endDate = enddate1)

df_2 = pd.read_sql(B, conMB)
df_2


# In[95]:


S = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,87)
 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,87)

) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' """.format(startDate = startdate, endDate = enddate)

df2 = pd.read_sql(S, conMB)
df2


# In[96]:


C = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,87)
 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,87)

) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' """.format(startDate = startdate1, endDate = enddate1)

df_3 = pd.read_sql(C, conMB)
df_3


# In[97]:


T = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,115)
 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,115)

) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' AND PushToPartner NOT LIKE 'DirectlyPush' """.format(startDate = startdate, endDate = enddate)

df3 = pd.read_sql(T, conMB)
df3


# In[98]:


D = """select EVENT, TAT,RF_R_ID, providername,IsPickUpStoreRequest FROM
(select distinct C.RF_R_ID,DATEDIFF(MINUTE, date2, date1) AS TAT, concat(C.SM_STATUSDESC,'_',D.SM_STATUSDESC) as event ,C.providername,C.IsPickUpStoreRequest from 

(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC) RowNumber, sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date1,RF_StatusId ,P.ProviderName,
                         JSON_VALUE(CONVERT(varchar(MAX), tr.R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
                         FROM MediMarket..tblRequestFlow A (nolock) left  join rpt.MediMarketRequest P (nolock) on A.RF_R_ID=P.RequestId left join 
                         MediMarket.dbo.tblrequest tr  with (nolock) on tr.R_Id=P.RequestId
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=A.RF_StatusId
                         
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,115)
 
  ) C  
  join 
(select ROW_NUMBER() OVER(PARTITION BY  RF_R_ID
                         ORDER BY RF_CreatedOn DESC)  Ranknum , sm.SM_STATUSDESC, RF_R_ID,RF_CreatedOn as date2,RF_StatusId 
                         FROM MediMarket..tblRequestFlow B (nolock) 
                         left join MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) on sm.SM_ID=B.RF_StatusId
WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
AND RF_StatusId in (91,115)

) D
on C.RF_R_ID=D.RF_R_ID and C.RowNumber+1=D.Ranknum
)E left join (select RequestId, 
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PushToPartner') = 'true' THEN 'DirectlyPush' else 'opsQueue'  END as 'PushToPartner',
CASE WHEN JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.CourierPincode') is null THEN 'No' else 'Yes'  END as 'CourierOrder',

CASE
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
                                      END AS "Final_Status"  from rpt.MediMarketRequest(nolock)
                                      LEFT JOIN dbo.tblRequest (nolock) tbr ON RequestId = tbr.R_Id
                                      )F on RF_R_ID=F.RequestId 
                                      where Final_Status='Delivered' and ProviderName='Pharmeasy' and CourierOrder='No'
                                      and IsPickUpStoreRequest='false' AND PushToPartner NOT LIKE 'DirectlyPush' """.format(startDate = startdate1, endDate = enddate1)

df_4 = pd.read_sql(D, conMB)
df_4


# In[ ]:





# In[ ]:





# In[99]:


df1['EVENT'].value_counts()


# In[100]:


df_2['EVENT'].value_counts()


# In[101]:


q1=df1['TAT'].where(df1['EVENT']=='Delivered_New Cashless Request')
q1.quantile(0.5)
q1.quantile(0.6)
q1.quantile(0.7)
q1.quantile(0.8)
q1.quantile(0.9)
q1.quantile(0.95)
q1.quantile(0.99)
q1.quantile(1.0)


# In[102]:


q_1=df_2['TAT'].where(df_2['EVENT']=='Delivered_New Cashless Request')







# In[103]:


q_1.quantile(0.5)


# In[104]:


q_1.quantile(0.6)


# In[105]:


q_1.quantile(0.7)


# In[106]:


q_1.quantile(0.8)


# In[107]:


q_1.quantile(0.9)


# In[108]:


q_1.quantile(0.95)


# In[109]:


q_1.quantile(0.99)


# In[110]:



q_1.quantile(1.0)


# In[111]:


q1.quantile(0.5)


# In[112]:


q1.quantile(0.6)


# In[113]:


q1.quantile(0.7)


# In[114]:


q1.quantile(0.8)


# In[115]:


q1.quantile(0.9)


# In[116]:


q1.quantile(0.95)


# In[117]:


q1.quantile(0.99)


# In[118]:


q1.quantile(1.0)


# In[119]:


df2['EVENT'].value_counts()


# In[120]:


df_3['EVENT'].value_counts()


# In[121]:


q_3=df_3['TAT'].where(df_3['EVENT']=='Delivered_Placed with Partner')
q_3.quantile(0.5)






# In[122]:


q_3.quantile(0.6)


# In[123]:


q_3.quantile(0.7)


# In[124]:


q_3.quantile(0.8)


# In[125]:


q_3.quantile(0.9)


# In[126]:


q_3.quantile(0.95)


# In[127]:



q_3.quantile(0.99)


# In[128]:


q_3.quantile(1.0)


# In[129]:


q2=df2['TAT'].where(df2['EVENT']=='Delivered_Placed with Partner')
q2.quantile(0.5)
q2.quantile(0.6)
q2.quantile(0.7)
q2.quantile(0.8)
q2.quantile(0.9)
q2.quantile(0.95)
q2.quantile(0.99)
q2.quantile(1.0)


# In[130]:


q2.quantile(0.5)


# In[131]:


q2.quantile(0.6)


# In[132]:


q2.quantile(0.7)


# In[133]:


q2.quantile(0.8)


# In[134]:


q2.quantile(0.9)


# In[135]:


q2.quantile(0.95)


# In[136]:


q2.quantile(0.99)


# In[137]:


q2.quantile(1.0)


# In[138]:


df3['EVENT'].value_counts()


# In[139]:


df_4['EVENT'].value_counts()


# In[140]:


q_4=df_4['TAT'].where(df_4['EVENT']=='Placed with Partner_New Cashless Request')
q_4.quantile(0.5)







# In[141]:


q_4.quantile(0.6)


# In[142]:


q_4.quantile(0.7)


# In[143]:


q_4.quantile(0.8)


# In[144]:


q_4.quantile(0.9)


# In[145]:


q_4.quantile(0.95)


# In[146]:


q_4.quantile(0.99)


# In[147]:


q_4.quantile(1.0)


# In[148]:


q3=df3['TAT'].where(df3['EVENT']=='Placed with Partner_New Cashless Request')
q3.quantile(0.5)
q3.quantile(0.6)
q3.quantile(0.7)
q3.quantile(0.8)
q3.quantile(0.9)
q3.quantile(0.95)
q3.quantile(0.99)
q3.quantile(1.0)


# In[149]:


q3.quantile(0.5)


# In[150]:


q3.quantile(0.6)


# In[151]:


q3.quantile(0.7)


# In[152]:


q3.quantile(0.8)


# In[153]:


q3.quantile(0.9)


# In[154]:


q3.quantile(0.95)


# In[155]:


q3.quantile(0.99)


# In[156]:


q3.quantile(1.0)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[157]:


data1 = [{'50 Per': q1.quantile(0.5), '60 Per': q1.quantile(0.6),'70 Per': q1.quantile(0.7),'80 Per': q1.quantile(0.8),'90 Per': q1.quantile(0.9),'99 Per': q1.quantile(0.99),'100 Per': q1.quantile(1.0)}]
        


# In[158]:


dff1 = pd.DataFrame(data1, index =['Delivered_New Cashless Request'
                                 ],
                   columns =['50 Per', '60 Per','70 Per','80 Per','90 Per','99 Per','100 Per'])


# In[159]:


dff1


# In[160]:


data1 = [{'50 Per': p1.quantile(0.5), '60 Per': p1.quantile(0.6),'70 Per': p1.quantile(0.7),'80 Per': p1.quantile(0.8),'90 Per': p1.quantile(0.9),'99 Per': p1.quantile(0.99),'100 Per': p1.quantile(1.0)}
        ,{'50 Per': p2.quantile(0.5), '60 Per': p2.quantile(0.6),'70 Per': p2.quantile(0.7),'80 Per': p2.quantile(0.8),'90 Per': p2.quantile(0.9),'99 Per': p2.quantile(0.99),'100 Per': p2.quantile(1.0)},
       {'50 Per': p3.quantile(0.5), '60 Per': p3.quantile(0.6),'70 Per': p3.quantile(0.7),'80 Per': p3.quantile(0.8),'90 Per': p3.quantile(0.9),'99 Per': p3.quantile(0.99),'100 Per': p3.quantile(1.0)},
       {'50 Per': p4.quantile(0.5), '60 Per': p4.quantile(0.6),'70 Per': p4.quantile(0.7),'80 Per': p4.quantile(0.8),'90 Per': p4.quantile(0.9),'99 Per': p4.quantile(0.99),'100 Per': p4.quantile(1.0)},
       {'50 Per': p5.quantile(0.5), '60 Per': p5.quantile(0.6),'70 Per': p5.quantile(0.7),'80 Per': p5.quantile(0.8),'90 Per': p5.quantile(0.9),'99 Per': p5.quantile(0.99),'100 Per': p5.quantile(1.0)}
       ]


# In[161]:


data2=[{'50 Per': q1.quantile(0.5), '60 Per': q1.quantile(0.6),'70 Per': q1.quantile(0.7),'80 Per': q1.quantile(0.8),'90 Per': q1.quantile(0.9),'99 Per': q1.quantile(0.99),'100 Per': q1.quantile(1.0)}
       ,{'50 Per': q2.quantile(0.5), '60 Per': q2.quantile(0.6),'70 Per': q2.quantile(0.7),'80 Per': q2.quantile(0.8),'90 Per': q2.quantile(0.9),'99 Per': q2.quantile(0.99),'100 Per': q2.quantile(1.0)}
       ,{'50 Per': q3.quantile(0.5), '60 Per': q3.quantile(0.6),'70 Per': q3.quantile(0.7),'80 Per': q3.quantile(0.8),'90 Per': q3.quantile(0.9),'99 Per': q3.quantile(0.99),'100 Per': q3.quantile(1.0)}
       ]


# In[162]:


data3 = [{'50 Per': p_1.quantile(0.5), '60 Per': p_1.quantile(0.6),'70 Per': p_1.quantile(0.7),'80 Per': p_1.quantile(0.8),'90 Per': p_1.quantile(0.9),'99 Per': p_1.quantile(0.99),'100 Per': p_1.quantile(1.0)}
        ,{'50 Per': p_2.quantile(0.5), '60 Per': p_2.quantile(0.6),'70 Per': p_2.quantile(0.7),'80 Per': p_2.quantile(0.8),'90 Per': p_2.quantile(0.9),'99 Per': p_2.quantile(0.99),'100 Per': p_2.quantile(1.0)},
       {'50 Per': p_3.quantile(0.5), '60 Per': p_3.quantile(0.6),'70 Per': p_3.quantile(0.7),'80 Per': p_3.quantile(0.8),'90 Per': p_3.quantile(0.9),'99 Per': p_3.quantile(0.99),'100 Per': p_3.quantile(1.0)},
       {'50 Per': p_4.quantile(0.5), '60 Per': p4.quantile(0.6),'70 Per': p_4.quantile(0.7),'80 Per': p_4.quantile(0.8),'90 Per': p_4.quantile(0.9),'99 Per': p_4.quantile(0.99),'100 Per': p_4.quantile(1.0)},
       {'50 Per': p5.quantile(0.5), '60 Per': p5.quantile(0.6),'70 Per': p_5.quantile(0.7),'80 Per': p_5.quantile(0.8),'90 Per': p_5.quantile(0.9),'99 Per': p_5.quantile(0.99),'100 Per': p_5.quantile(1.0)}
       ]


# In[163]:


data4=[{'50 Per': q_1.quantile(0.5), '60 Per': q_1.quantile(0.6),'70 Per': q_1.quantile(0.7),'80 Per': q_1.quantile(0.8),'90 Per': q_1.quantile(0.9),'99 Per': q_1.quantile(0.99),'100 Per': q_1.quantile(1.0)}
       ,{'50 Per': q_3.quantile(0.5), '60 Per': q_3.quantile(0.6),'70 Per': q_3.quantile(0.7),'80 Per': q_3.quantile(0.8),'90 Per': q_3.quantile(0.9),'99 Per': q_3.quantile(0.99),'100 Per': q_3.quantile(1.0)}
       ,{'50 Per': q_4.quantile(0.5), '60 Per': q_4.quantile(0.6),'70 Per': q_4.quantile(0.7),'80 Per': q_4.quantile(0.8),'90 Per': q_4.quantile(0.9),'99 Per': q_4.quantile(0.99),'100 Per': q_4.quantile(1.0)}
       ]


# In[164]:


dff1 = pd.DataFrame(data1, index =['Confirmed by Retailer_Placed with Partner','Invoice Generated_Invoice Pending','Out for Delivery_Invoice Generated',
'Invoice Pending_Confirmed by Retailer','Delivered_Out for Delivery']
                   ,columns =['50 Per', '60 Per','70 Per','80 Per','90 Per','99 Per','100 Per'])


# In[165]:


dff2 = pd.DataFrame(data2, index =['Delivered_New Cashless Request','Delivered_Placed with Partner','Placed with Partner_New Cashless Request']
                   ,columns =['50 Per', '60 Per','70 Per','80 Per','90 Per','99 Per','100 Per'])


# In[166]:


dff3 = pd.DataFrame(data3, index =['Confirmed by Retailer_Placed with Partner','Invoice Generated_Invoice Pending','Out for Delivery_Invoice Generated',
'Invoice Pending_Confirmed by Retailer','Delivered_Out for Delivery']
                   ,columns =['50 Per', '60 Per','70 Per','80 Per','90 Per','99 Per','100 Per'])


# In[167]:


dff4 = pd.DataFrame(data4, index =['Delivered_New Cashless Request','Delivered_Placed with Partner','Placed with Partner_New Cashless Request']
                   ,columns =['50 Per', '60 Per','70 Per','80 Per','90 Per','99 Per','100 Per'])


# In[168]:


dff1= dff1/60


# In[169]:


dff1


# In[170]:


dff2=dff2/60


# In[171]:


dff2


# In[172]:


dff3=dff3/60
dff3


# In[173]:


dff4=dff4/60
dff4


# In[174]:


dff1.reset_index(level=0, inplace=True)


# In[175]:


dff1=dff1.rename(columns={'index': 'Status (PE-HD)'})


# In[176]:


dff1


# In[177]:


dff2.reset_index(level=0, inplace=True)


# In[178]:


dff2=dff2.rename(columns={'index': 'Status (PE-HD)'})


# In[179]:


dff2


# In[180]:


dff3.reset_index(level=0, inplace=True)


# In[181]:


dff3=dff3.rename(columns={'index': 'Status (PE-HD)'})


# In[182]:


dff3


# In[183]:


dff4.reset_index(level=0, inplace=True)


# In[184]:


d1


# In[185]:


d1.reset_index(level=0, inplace=True)
d2.reset_index(level=0, inplace=True)


# In[186]:


d1


# In[187]:


d1=d1.rename(columns={'index': 'Event','EVENT': 'Count'})


# In[188]:


d2=d2.rename(columns={'index': 'Event','EVENT': 'Count'})


# In[189]:


d1


# 

# In[190]:


dff4=dff4.rename(columns={'index': 'Status (PE-HD)'})
dff4


# In[191]:


dff1


# In[192]:


dff2


# In[193]:


dff3


# In[194]:


dff4


# In[195]:


dff4=dff4.round(decimals=2)
dff3=dff3.round(decimals=2)
dff2=dff2.round(decimals=2)
dff1=dff1.round(decimals=2)


# In[196]:


dff1


# In[197]:


player_order1=pd.Categorical([ 'Confirmed by Retailer_Placed with Partner' ,'Invoice Pending_Confirmed by Retailer','Invoice Generated_Invoice Pending','Out for Delivery_Invoice Generated' ,'Delivered_Out for Delivery'],
              ordered=True)


# In[ ]:





# In[198]:


def sorter(column):
    reorder = [ 'Confirmed by Retailer_Placed with Partner', 'Invoice Pending_Confirmed by Retailer','Invoice Generated_Invoice Pending' ,'Out for Delivery_Invoice Generated' ,'Delivered_Out for Delivery']
    # This also works:
    # mapper = {name: order for order, name in enumerate(reorder)}
    # return column.map(mapper)
    cat = pd.Categorical(column, categories=reorder, ordered=True)
    return pd.Series(cat)

dff1 = dff1.sort_values(by="Status (PE-HD)", key=sorter)
dff3 = dff3.sort_values(by="Status (PE-HD)", key=sorter)


# In[199]:


dff1


# In[200]:


dff1


# In[201]:


dff3


# In[202]:



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


# In[203]:


dffhtml1= dff1.to_html(justify ='center', index=False).replace('\n','')
dffhtml2= dff2.to_html(justify ='center', index=False).replace('\n','')
dffhtml3= dff3.to_html(justify ='center', index=False).replace('\n','')
dffhtml4= dff4.to_html(justify ='center', index=False).replace('\n','')
d1= d1.to_html(justify ='center', index=False).replace('\n','')
d2= d2.to_html(justify ='center', index=False).replace('\n','')

print(dffhtml1)
print(dffhtml2)
print(dffhtml3)
print(dffhtml4)


# In[204]:


html = "<h2>Current Week absolute numbers</h2>" + d1 + "<h2>Current Week</h2>" + dffhtml2 + '<br/><br/>' + "<h2>Current Week</h2>" + dffhtml1 + '<br/><br/>' + "<h2>Last Week absolute numbers</h2>" + d2 + "<h2>Last Week</h2>" + dffhtml4 + '<br/><br/>' + "<h2>Last Week</h2>" + dffhtml3
html
today = datetime.date.today()
yesterday = (today - datetime.timedelta(days=1)).strftime('%d-%b-%Y')
print(yesterday)
    
Heading = 'TAT in hours for Current and Previous week (PHARMEASY HOME DELIVERY)' + '(' + yesterday + ')'
recipients = ["deepak.nagar@medibuddy.in","sivathanu.k@docsapp.in","adhithya.parthasarathi@medibuddy.in","vishwanath.ms@medibuddy.in","ronak.shah@medibuddy.in","omprakash.n@medibuddy.in"]

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





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




