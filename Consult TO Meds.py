#!/usr/bin/env python
# coding: utf-8

# In[2]:



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



# In[ ]:





# In[ ]:





# In[4]:


enddate = datetime.date.today() 
startdate = datetime.date.today()  - datetime.timedelta(days=5)
print(startdate)
print(enddate)


# In[5]:


node="mysql+pymysql://analyticswriterole:a1anllytis2345writer123@docsapp-prod-replica.cw68gbfmumcn.ap-south-1.rds.amazonaws.com/node"
connection = create_engine(node)
conRnode = connection.connect()
print('connected to readReplica - node')


# In[42]:


h="""SELECT A.RequestID,
          DATEDIFF(HOUR, A.ApptCreatedDate, D.DeliveredDate) AS TAT1
        
   FROM
     (SELECT RequestID,
             ApptCreatedDate
      FROM rpt.MediMarketRequest (nolock)
      INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
      AND isactive=1
      LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
      AND RF_IsActive = 1
      AND RF_StatusId IN (86,
                          87)
      WHERE ContractTypeId IN (10307,
                               10390)
        AND ApptCreatedDate>='{startDate}'
  AND ApptCreatedDate < '{endDate}') A
   LEFT JOIN
     (SELECT RF_R_ID,
             RF_CreatedOn AS DeliveredDate
      FROM dbo.tblRequestFlow (NOLOCK)
      WHERE RF_CreatedOn>='{startDate}'
  AND RF_CreatedOn < '{endDate}'
        AND RF_StatusId IN (86,
                            87)) D ON A.RequestID = D.RF_R_ID where DATEDIFF(HOUR, A.ApptCreatedDate, D.DeliveredDate) IS NOT null""".format(startDate = startdate, endDate = enddate)
DF = pd.read_sql(h, conMB)
DF


# In[43]:


p="""SELECT convert(date, fb.CREATEDON) AS date ,
       fb.RequestId,
       fb.OverallRating,
       ans.primarydata AS feedback,
       fb.remarks,
       c.ContactNo
FROM MediMarket..tblFeedbackReport(NOLOCK) fb
JOIN MediMarket..tblFeedbackUserResponse(NOLOCK) r ON fb.ReferenceVal = r.ReferenceVal
JOIN MediMarket..tblFeedbackQandA(NOLOCK) ans ON r.answer_id = ans.primaryid
JOIN
  (SELECT DISTINCT A.* ,
                   CASE
                       WHEN rankmeds>1 THEN 'true'
                       ELSE 'false'
                   END AS ReOrder ,
                   C.FirstOrderDate
   FROM
     (SELECT DISTINCT RequestID ,
                      OrderId -- ContractTypeName

                             ,
                             sm.SM_StatusDesc AS "Appt Status" ,
                             ApptCreatedDate -- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2

                                            ,
                                            ProviderName ,
                                            PackageName ,
                                            CorporateName AS CorporateName_Rpt ,
                                            E_FullName AS CorporateName ,
                                            MAID ,
                                            EmailId ,
                                            EmployeeID ,
                                            Relationship ,
                                            Gender ,
                                            CASE WHEN ContractTypeName='MP_PHARMACY'
      OR ContractTypeName='Pharmacy_Mediassist' THEN convert(float, JSON_VALUE(convert(varchar(MAX),Properties),'$.cashlessAmount')) ELSE 0 END AS PharmacyCashless ,
                                                                                                                                                   CASE WHEN ContractTypeName='MP_PHARMACY'
      OR ContractTypeName='Pharmacy_Mediassist' THEN convert(float, JSON_VALUE(convert(varchar(MAX),Properties),'$.lineAmount')) ELSE 0 END AS PharmacyCash ,
                                                                                                                                               MAWallet ,
                                                                                                                                               MBWallet ,
                                                                                                                                               CorpWallet -- , oft.Amount as TotalAmount

                                                                                                                                                         ,
                                                                                                                                                         CASE WHEN oft.Amount IS NOT NULL THEN oft.Amount ELSE rw.RW_PaybleAmount END AS TotalAmount ,
cast(mmr.isactive AS varchar) AS IsActive ,
PatientName ,
O_Billing_EmailId ,
Pharma_OrderID AS [Partner order id] ,
Remarks ,
ContactNo ,
JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.addressLine1') AS AddressLine1 ,
JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.addressLine2') AS AddressLine2 ,
ClientCity ,
JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PinCode') AS PinCode ,
CASE WHEN rf.RF_Id IS NOT NULL THEN rf.RF_CreatedOn END AS DeliveryTime0 ,
                                                           CASE WHEN rf.RF_Id IS NULL
      AND datediff(DD,PD_Createdon,getdate()) > 99 THEN datediff(DD,PD_Createdon,getdate()) * 9 * 60 * 6 WHEN rf.RF_Id IS NOT NULL
      AND datediff(DD,PD_Createdon,rf.RF_CreatedOn) > 99 THEN datediff(DD,PD_Createdon,rf.RF_CreatedOn) * 9 * 60 * 6 ELSE
( SELECT SUM(TATMINUTES)
 FROM ProviderMaster.pmr.GetTAT(PD_CreatedOn, COALESCE(rf.RF_CreatedOn, GETDATE()), '1', '09:00', '18:00')) END [ProviderOperation_TAT(InMinutes)] -- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless

                                                                                                                                                  ,
                                                                                                                                                  CASE WHEN lower(ContractTypeName)='mp_pharmacy'
      AND JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' THEN 'CashlessOrder' WHEN lower(ContractTypeName)!='mp_pharmacy'
      AND (MAWallet>0
           OR JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') THEN 'CashlessOrder' ELSE 'CashOrder' END AS isCashless ,
                                                                                                                             LastUpdateDate ,
                                                                                                                             AppointmentStatus ,
                                                                                                                             CASE WHEN AppointmentStatus = 'Attended' THEN 'Delivered' WHEN AppointmentStatus = 'Callback Required' THEN 'InProcess' WHEN AppointmentStatus = 'Cancelled by Medical Center' THEN 'Cancelled' WHEN AppointmentStatus = 'Cancelled by Ops' THEN 'Cancelled' WHEN AppointmentStatus = 'Cancelled by User' THEN 'Cancelled' WHEN AppointmentStatus = 'Confirmed by Medical Center' THEN 'InProcess' WHEN AppointmentStatus = 'Confirmed by Ops' THEN 'InProcess' WHEN AppointmentStatus = 'Confirmed by Retailer' THEN 'InProcess' WHEN AppointmentStatus = 'Customer Awaited' THEN 'InProcess' WHEN AppointmentStatus = 'Delivered' THEN 'Delivered' WHEN AppointmentStatus = 'Delivered Partial' THEN 'Delivered' WHEN AppointmentStatus = 'Delivery attempt failed' THEN 'InProcess' WHEN AppointmentStatus = 'Delivery Executive Assigned' THEN 'InProcess' WHEN AppointmentStatus = 'Info submitted' THEN 'Delivered' WHEN AppointmentStatus = 'Invoice Generated' THEN 'InProcess' WHEN AppointmentStatus = 'Invoice Pending' THEN 'InProcess' WHEN AppointmentStatus = 'MACancel' THEN 'Cancelled' WHEN AppointmentStatus = 'New Appointment Request' THEN 'InProcess' WHEN AppointmentStatus = 'New Cashless Request' THEN 'InProcess' WHEN AppointmentStatus = 'Not Attended' THEN 'InProcess' WHEN AppointmentStatus = 'On Hold' THEN 'InProcess' WHEN AppointmentStatus = 'Out for Delivery' THEN 'InProcess' WHEN AppointmentStatus = 'Placed with Partner' THEN 'InProcess' WHEN AppointmentStatus = 'Postponed' THEN 'InProcess' WHEN AppointmentStatus = 'Rejected by Moderator' THEN 'Cancelled' WHEN AppointmentStatus = 'Rejected by Retailer' THEN 'Cancelled' WHEN AppointmentStatus = 'Rescheduled by Ops' THEN 'InProcess' WHEN AppointmentStatus = 'Rescheduled by User' THEN 'InProcess' WHEN AppointmentStatus = 'Settled' THEN 'Delivered' WHEN AppointmentStatus = 'UserCancel' THEN 'Cancelled' WHEN AppointmentStatus = 'System Confirmed' THEN 'InProcess' WHEN AppointmentStatus = 'Rescheduled by Medical Center' THEN 'InProcess' WHEN AppointmentStatus = 'Cancelled' THEN 'Cancelled' WHEN AppointmentStatus = 'Confirmed' THEN 'InProcess' WHEN AppointmentStatus = 'Delivered' THEN 'Delivered' WHEN AppointmentStatus = 'Processed' THEN 'Delivered' WHEN AppointmentStatus = 'Ready To Deliver' THEN 'InProcess' WHEN AppointmentStatus = 'Received' THEN 'InProcess' WHEN AppointmentStatus = 'Medicine Delivered' THEN 'Delivered' WHEN AppointmentStatus = 'Medicine Delivery in Progess' THEN 'InProcess' END AS "Final Status" ,
JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.isPickUpfromStore') AS IsPickUpStoreRequest ,
CASE WHEN PD_CreatedOn IS NULL THEN 'false' ELSE 'true' END AS PushToPartner ,
                                                               PD_CreatedOn PartnerPushDate ,
                                                                            CASE WHEN JSON_VALUE(convert(varchar(MAX),Properties),'$.additionals.CourierPincode') IS NOT NULL THEN 'Yes' ELSE 'No' END AS CourierOrder ,
CASE WHEN R_Blob LIKE '%source\":\"web%' THEN 'Web' WHEN R_Blob LIKE '%source\":\"AndroidMB%' THEN 'Android' WHEN R_Blob LIKE '%source\":\"iOSMB%' THEN 'iOS' ELSE 'No Info Available' END AS Platform ,
                                                                                                                                                                                              CASE WHEN R_Blob LIKE '%PharmaOrderPrescriptionFiles%' THEN 1 ELSE 0 END AS PrescriptionUploaded ,
CASE WHEN R_Blob LIKE '%ONLYOTC%' THEN 1 ELSE 0 END AS ONLYOTC -- , CASE
--       WHEN refill.RO_RequestId > 1 THEN 1
--     ELSE 0
--       END AS RefillOrder

                                                              ,
                                                              CASE WHEN properties LIKE '%SystemRefillSubscription%' THEN 1 ELSE 0 END AS RefillOrder ,
                                                                                                                                          CASE WHEN properties LIKE '%PharmEasyTeleconsultation%' THEN 1 ELSE 0 END AS PharmEasyConsultation ,
JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.TrackingURL') AS TrackingURL ,
CASE WHEN TransactionId IS NOT NULL THEN TransactionId ELSE rw.RW_TrackingNumber END AS ClaimReferenceNumber ,
                                                                                        JSON_VALUE(CONVERT(varchar(MAX), properties), '$.additionals.PharmaOrderPrescriptionFiles') AS PharmaOrderPrescriptionFiles
      FROM rpt.MediMarketRequest (nolock) mmr
      INNER JOIN MediMarket.dbo.tblrequest tr WITH (nolock) ON tr.R_Id=RequestID
      AND isactive=1
      LEFT JOIN MediMarket.dbo.tblworkflowstatusmaster sm WITH (nolock) ON appointmentstatusid=sm.sm_id
      LEFT JOIN Medimarket.dbo.tblorder(nolock) ord ON ord.o_id=OrderId
      LEFT JOIN MediAuth2..tblapplicationuser tau WITH(nolock) ON tau.tau_id=R_createdby
      LEFT JOIN ProviderMaster.pmr.ENTITY et WITH (nolock) ON et.E_id= TAU_ProviderMasterEntityId
      LEFT JOIN
(SELECT O_id,
        MBWallet,
        CorpWallet,
        CouponCode,
        PaymentGateway,
        MAWallet,
        HomeSampleCollectionCharges
 FROM
   ( SELECT Amount,
            PaymentSource,
            O_id
    FROM Medimarket.marketplace.orderfinancial (nolock) ) d pivot ( MAX(Amount)
                                                                   FOR PaymentSource IN (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges) ) piv )AS tt ON tt.O_ID=OrderId
      LEFT JOIN medimarket..tblOrderItem oit ON oit.OI_Id = orderitemid
      LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK) ON MA_RequestID = R_ID
      LEFT JOIN MediMarket..tblRequestFlow(nolock) rf ON R_Id = rf.RF_R_ID
      AND RF_IsActive = 1
      AND RF_StatusId IN (86,
                          87)
      LEFT JOIN
(SELECT O_Id,
        TransactionId,
        MAX(Amount) AS Amount
 FROM Medimarket.marketplace.orderfinancial (nolock)
 GROUP BY O_Id,
          TransactionId) oft ON oft.O_ID=OrderId
      LEFT JOIN
(SELECT RW_R_Id,
        RW_TrackingNumber,
        RW_PaybleAmount
 FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId -- LEFT JOIN MarketPlace.RefillOrder refill ON RequestId = refill.RO_RequestId

WHERE ContractTypeId IN (-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,
10390 -- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)
AND ApptCreatedDate + 10 >= '{startDate}' 
AND ApptCreatedDate + 10 < '{endDate}'
AND isactive = 1) A
   LEFT JOIN
(SELECT ContactNo,
        RequestID,
        rank() over (partition BY ContactNo
                     ORDER BY RequestID) AS rankmeds
 FROM rpt.MediMarketRequest (nolock)
 WHERE ContractTypeId IN (10307,
                          10390)
   AND IsActive = 1) B ON A.RequestID = B.RequestID
   LEFT JOIN
(SELECT ContactNo,
        min(ApptCreatedDate) AS FirstOrderDate
 FROM rpt.MediMarketRequest (nolock)
 WHERE ContractTypeId IN (10307,
                          10390)
   AND IsActive = 1
 GROUP BY ContactNo) C ON A.ContactNo = C.ContactNo) AS c ON fb.RequestId = c.RequestID
AND ans.CategoryTypeId IN (10390)
WHERE fb.createdon >= '{startDate}'
AND fb.createdon < '{endDate}'
GROUP BY convert(date, fb.CREATEDON),
     OverallRating,
     fb.remarks,
     fb.RequestId,
     primarydata,
     c.ContactNo,
     c.EmailId""".format(startDate = startdate, endDate = enddate)
DFp = pd.read_sql(p, conMB)
DFp


# In[44]:


DFp.to_csv('OPSCSAT_7.csv')


# In[45]:


DF.to_csv('TAT_RequestID.csv')


# In[46]:


DF1=pd.read_csv('DA.csv')


# In[ ]:


DF1


# In[ ]:


yy="""
select 
    RequestID, CASE
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
                                      END AS "Final_Status"
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
""".format(startDate = startdate, endDate = enddate)


DFF4 = pd.read_sql(yy, conMB)
DFF4


# In[ ]:


ff="""select id as RequestID, case when refunded=0 then 'Cancelled' else 'Delivered' end as Final_Status from orders where createdat>'{startDate}' """.format(startDate = startdate, endDate = enddate)
DFF3 = pd.read_sql(ff, node)
DFF3



# In[ ]:


dff5=DFF4.append(DFF3)


# In[ ]:


dff5


# In[ ]:


df3=dff5.merge(DF1,how='inner', left_on='RequestID', right_on='Request ID')


# In[ ]:


df3.to_csv('Status.csv')


# In[ ]:


df3['RequestID'].nunique()


# In[ ]:


kj="""select distinct A.*
, 
case when PAYMENTSOURCE = 'PaymentGateway' then 1
ELSE 0 END AS PG_Flag,
case when rankmeds>1 then 'true' else 'false' end as ReOrder
, 
case when PAYMENTSOURCE = 'PaymentGateway' then Amount
ELSE Null END AS PGAmtPaid,
case when rankmeds>1 then 'true' else 'false' end as ReOrder,

C.FirstOrderDate
from (
select distinct
RequestID,
PAYMENTSOURCE
, OrderId
-- ContractTypeName
, sm.SM_StatusDesc as "Appt Status"
, ApptCreatedDate
-- , AppointmentApprovalDate
-- , DATEDIFF(day, AppointmentApprovalDate, ApptCreatedDate) as ApptApprovalTAT
-- , ApptDate
-- , DATEDIFF(day, ApptDate, AppointmentApprovalDate) as ApptTAT_2
, ProviderName
, PackageName,
Amount
, CorporateName as CorporateName_Rpt
, E_FullName as CorporateName
, MAID
, EmailId
, EmployeeID
, Relationship
, Gender
, case when ContractTypeName='MP_PHARMACY' or ContractTypeName='Pharmacy_Mediassist' then convert(float, JSON_VALUE(convert(varchar(max),Properties),'$.cashlessAmount'))  else 0 end as PharmacyCashless
, case when ContractTypeName='MP_PHARMACY' or ContractTypeName='Pharmacy_Mediassist' then convert(float, JSON_VALUE(convert(varchar(max),Properties),'$.lineAmount')) else 0 end as PharmacyCash
, MAWallet
, MBWallet
, CorpWallet
-- , oft.Amount as TotalAmount
, CASE
      WHEN oft.Amount IS NOT NULL THEN oft.Amount
    ELSE rw.RW_PaybleAmount
       END AS TotalAmount
, cast(mmr.isactive as varchar) as IsActive
, PatientName
, O_Billing_EmailId
, Pharma_OrderID as [Partner order id]
, Remarks
, ContactNo
, JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.addressLine1') as AddressLine1
, JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.addressLine2') as AddressLine2
, ClientCity
, JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.PinCode') as PinCode
, CASE
		WHEN rf.RF_Id IS NOT NULL THEN rf.RF_CreatedOn
	END AS DeliveryTime0 

, case		when rf.RF_Id is null and datediff(DD,PD_Createdon,getdate()) > 99 then datediff(DD,PD_Createdon,getdate()) * 9 * 60 * 6  
			when rf.RF_Id is not null and datediff(DD,PD_Createdon,rf.RF_CreatedOn) > 99 then datediff(DD,PD_Createdon,rf.RF_CreatedOn) * 9 * 60 * 6  
   
   else
   (
   SELECT	SUM(TATMINUTES) FROM ProviderMaster.pmr.GetTAT(PD_CreatedOn, COALESCE(rf.RF_CreatedOn, GETDATE()), '1', '09:00', '18:00') 
   )
   end
    [ProviderOperation_TAT(InMinutes)]
-- , JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless') AS IsCashless
, case when lower(ContractTypeName)='mp_pharmacy' and JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true' then 'CashlessOrder'        
        when lower(ContractTypeName)!='mp_pharmacy' and (MAWallet>0 or JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.isCashless')='true') then 'CashlessOrder'
        else 'CashOrder' end as isCashless
, LastUpdateDate
,  AppointmentStatus
,
case
when AppointmentStatus = 'Attended' then 'Delivered'
when AppointmentStatus = 'Callback Required' then 'InProcess'
when AppointmentStatus = 'Cancelled by Medical Center' then 'Cancelled'
when AppointmentStatus = 'Cancelled by Ops' then 'Cancelled'
when AppointmentStatus = 'Cancelled by User' then 'Cancelled'
when AppointmentStatus = 'Confirmed by Medical Center' then 'InProcess'
when AppointmentStatus = 'Confirmed by Ops' then 'InProcess'
when AppointmentStatus = 'Confirmed by Retailer' then 'InProcess'
when AppointmentStatus = 'Customer Awaited' then 'InProcess'
when AppointmentStatus = 'Delivered' then 'Delivered'
when AppointmentStatus = 'Delivered Partial' then 'Delivered'
when AppointmentStatus = 'Delivery attempt failed' then 'InProcess'
when AppointmentStatus = 'Delivery Executive Assigned' then 'InProcess'
when AppointmentStatus = 'Info submitted' then 'Delivered'
when AppointmentStatus = 'Invoice Generated' then 'InProcess'
when AppointmentStatus = 'Invoice Pending' then 'InProcess'
when AppointmentStatus = 'MACancel' then 'Cancelled'
when AppointmentStatus = 'New Appointment Request' then 'InProcess'
when AppointmentStatus = 'New Cashless Request' then 'InProcess'
when AppointmentStatus = 'Not Attended' then 'InProcess'
when AppointmentStatus = 'On Hold' then 'InProcess'
when AppointmentStatus = 'Out for Delivery' then 'InProcess'
when AppointmentStatus = 'Placed with Partner' then 'InProcess'
when AppointmentStatus = 'Postponed' then 'InProcess'
when AppointmentStatus = 'Rejected by Moderator' then 'Cancelled'
when AppointmentStatus = 'Rejected by Retailer' then 'Cancelled'
when AppointmentStatus = 'Rescheduled by Ops' then 'InProcess'
when AppointmentStatus = 'Rescheduled by User' then 'InProcess'
when AppointmentStatus = 'Settled' then 'Delivered'
when AppointmentStatus = 'UserCancel' then 'Cancelled'
when AppointmentStatus = 'System Confirmed' then 'InProcess'
when AppointmentStatus = 'Rescheduled by Medical Center' then 'InProcess'
when AppointmentStatus = 'Cancelled' then 'Cancelled'
when AppointmentStatus = 'Confirmed' then 'InProcess'
when AppointmentStatus = 'Delivered' then 'Delivered'
when AppointmentStatus = 'Processed' then 'Delivered'
when AppointmentStatus = 'Ready To Deliver' then 'InProcess'
when AppointmentStatus = 'Received' then 'InProcess'
when AppointmentStatus = 'Medicine Delivered' then 'Delivered'
when AppointmentStatus = 'Medicine Delivery in Progess' then 'InProcess'
end as "Final Status"


, JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.isPickUpfromStore') as IsPickUpStoreRequest
, case when PD_CreatedOn is null then 'false' else 'true' end as PushToPartner
, PD_CreatedOn PartnerPushDate

, case when   JSON_VALUE(convert(varchar(max),Properties),'$.additionals.CourierPincode') is not null then 'Yes' else 'No' end as CourierOrder

, CASE
      WHEN R_Blob LIKE '%source\":\"web%' THEN 'Web'
      WHEN R_Blob LIKE '%source\":\"AndroidMB%' THEN 'Android'
      WHEN R_Blob LIKE '%source\":\"iOSMB%' THEN 'iOS'
      ELSE 'No Info Available' END AS Platform
, CASE
      WHEN R_Blob LIKE '%PharmaOrderPrescriptionFiles%' THEN 1
    ELSE 0
       END AS PrescriptionUploaded
, CASE
      WHEN R_Blob LIKE '%ONLYOTC%' THEN 1
    ELSE 0
       END AS ONLYOTC
-- , CASE
--       WHEN refill.RO_RequestId > 1 THEN 1
--     ELSE 0
--       END AS RefillOrder
, CASE
      WHEN properties like '%SystemRefillSubscription%' THEN 1
    ELSE 0
       END AS RefillOrder
, CASE
      WHEN properties like '%PharmEasyTeleconsultation%' THEN 1
    ELSE 0
       END AS PharmEasyConsultation
, JSON_VALUE(CONVERT(varchar(MAX), R_Blob), '$.additionals.TrackingURL') as TrackingURL

, CASE
      WHEN TransactionId IS NOT NULL THEN TransactionId
    ELSE rw.RW_TrackingNumber
       END AS ClaimReferenceNumber

, JSON_VALUE(CONVERT(varchar(MAX), properties), '$.additionals.PharmaOrderPrescriptionFiles') as PharmaOrderPrescriptionFiles

from rpt.MediMarketRequest (nolock) mmr

inner join MediMarket.dbo.tblrequest tr  with (nolock)
 on  tr.R_Id=RequestID and isactive=1

left join MediMarket.dbo.tblworkflowstatusmaster sm with (nolock)
on appointmentstatusid=sm.sm_id

left join	Medimarket.dbo.tblorder(nolock) ord
on ord.o_id=OrderId
left join MediAuth2..tblapplicationuser tau with(nolock) on tau.tau_id=R_createdby 
left join  ProviderMaster.pmr.ENTITY  et with (nolock) on  et.E_id= TAU_ProviderMasterEntityId
left join 

				(select O_id, MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges
								from
									(
										select	Amount, PaymentSource,O_id
										from	Medimarket.marketplace.orderfinancial (nolock)
									 ) d
								pivot
								(
									 max(Amount)
									 for PaymentSource in (MBWallet, CorpWallet, CouponCode, PaymentGateway, MAWallet,HomeSampleCollectionCharges)
								) piv
								)as tt
				


				on tt.O_ID=OrderId 
left join medimarket..tblOrderItem oit
on oit.OI_Id = orderitemid

LEFT JOIN MediMarket.Provider.PharmaDetail(NOLOCK)
ON   MA_RequestID = R_ID

LEFT JOIN MediMarket..tblRequestFlow(nolock) rf
ON		R_Id = rf.RF_R_ID
and		RF_IsActive = 1
AND		RF_StatusId IN (86,87)

LEFT JOIN (select O_Id, TransactionId,PAYMENTSOURCE, max(Amount) as Amount from Medimarket.marketplace.orderfinancial (nolock) group by O_Id, TransactionId,PAYMENTSOURCE) oft on oft.O_ID=OrderId 
LEFT JOIN
  (SELECT RW_R_Id,
          RW_TrackingNumber,
          RW_PaybleAmount
   FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId

-- LEFT JOIN MarketPlace.RefillOrder refill ON RequestId = refill.RO_RequestId

where ContractTypeId in (
-- 10386,10389,10390,10401,10846,10847,10861,10949,12553,
10307,10390
-- 10389 -- MP_INVESTIGATION
-- 10401 -- MP_CONSULTATION
-- ,13465
)   
and ApptCreatedDate>='{startDate}' and ApptCreatedDate < '{endDate}'
and isactive = 1
) A
LEFT JOIN
(SELECT ContactNo,
          RequestID,
          rank() over (partition BY ContactNo
                      ORDER BY RequestID) AS rankmeds
  FROM rpt.MediMarketRequest (nolock)
  where ContractTypeId in (10307,10390) and IsActive = 1) B on A.RequestID = B.RequestID
LEFT JOIN
(SELECT ContactNo,
          min(ApptCreatedDate) as FirstOrderDate
  FROM rpt.MediMarketRequest (nolock)
  where ContractTypeId in (10307,10390) and IsActive = 1
  group by ContactNo) C on A.ContactNo = C.ContactNo """.format(startDate = startdate, endDate = enddate)

dfkj = pd.read_sql(kj, conMB)
dfkj


# In[ ]:


dfkj.to_csv('OPSPG_7.csv')


# In[ ]:


pj="""SELECT *,
       CASE
           WHEN (PMEntityId != '1018900'
                 OR PMEntityId IS NULL)
                AND (PaymentReceivedByPG > 0
                     OR Cash>0 or MBWallet >0)
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
                     OR CorpWallet >0 or MBWallet >0) THEN 'OOP with Wallet'
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
   GROUP BY PI_R_Id) pi ON mmr.RequestId = pi.PI_R_Id""".format(startDate = startdate, endDate = enddate)

dfpj = pd.read_sql(pj, conMB)
dfpj


# In[ ]:


dfpj.to_csv('OPSGMV_7.csv')


# In[ ]:


kk=""" Select A.R_Id, A.R_StatusID, A.R_Remarks, A.R_PatientName, B.TransactionId, B.Amount, B.CreatedOn as PaymentDate, B.PaymentSource from tblRequest(nolock) as A full join MediMarket.MarketPlace.OrderFinancial(nolock) as B on A.R_MappingId1=B.O_ID where B.PaymentSource='PaymentGateway' and A.R_ContractType = 10390 and A.R_CreatedOn > '2021-09-01' and A.R_Blob like '%PaymentReceivedByPG%' and A.R_CreatedByUser not like '%test%'"""
dfkk = pd.read_sql(kk, conMB)
dfkk


# In[ ]:


dfkk.to_csv('OPSRefund_7.csv')


# In[ ]:


ll="""select O_ID,O_BOOKINGDATE, O_FirstName,O_Billing_EmailId,O_FINALAMOUNT,O_DISCOUNTAMOUNT,O_TOTALPAYABLEAMOUNT,O_DISCOUNTCODE,O_STATUS,O_PAYMENTMODE from Medimarket.dbo.tblorder where O_DiscountCode like 'INDIA15'
and  O_BOOKINGDATE<='2021-08-01'"""
dfl = pd.read_sql(ll, conMB)
dfl


# In[4]:


Q = """ SELECT 
convert(date,apptcreateddate) as date,
       Platform,
       OrderType,
       
       count(RequestID) AS CountOfOrdersCreated
       
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
                            where platform like 'Android' and OrderType like 'AndroidMB'
GROUP BY convert(date,apptcreateddate),
         Platform,
         OrderType
ORDER BY 1,
         2,
         3""".format(startDate = startdate, endDate = enddate)

df = pd.read_sql(Q, conMB)
df


# In[5]:


P="""select DATE(A.createdat) as date,count(A.patient) as countt  from Consults A left join Patients B on A.patient =B.id
left join MedibuddyDocsappMappings C on C.patientid= A.patient
where A.createdat between '{startDate}' and '{endDate}' and valid = 1 and doctorresponsetime is not null 
and acceptedconsultation is not null and issummarized=1 AND medibuddyuserid IS NOT NULL and B.platform like 'Android' group by DATE(A.createdat) """.format(startDate = startdate, endDate = enddate)

df1 = pd.read_sql(P,node)
df1


# In[ ]:





# In[6]:


df3=df.merge(df1,how='inner', left_on='date', right_on='date')


# In[7]:


df3


# In[8]:


df3['Conversion']= round(df3['CountOfOrdersCreated']*100/df3['countt'],2)


# In[9]:


df4= pd.DataFrame(df3, columns=['date','countt','CountOfOrdersCreated','Conversion'])


# In[10]:


df4


# In[11]:


df4.columns=['date','Consults','MedsOrders','Conv_Percent']


# In[12]:


df4


# In[13]:


df4.to_csv('C2M.csv')


# In[14]:


ee="""select  'Active MB Gold users' as tag ,count(distinct A.patientid) as count_ from Subscription  A left join Consults B on B.patient=A.patientid
left join MedibuddyDocsappMappings C on C.patientid= B.patient
where A.validtill>='{startDate}' and medibuddyuserid is not null""".format(startDate = startdate, endDate = enddate)
DFG = pd.read_sql(ee, node)
DFG


# In[6]:


tt="""select   A.patientid,medibuddyuserid from Subscription A left join Consults B on B.patient=A.patientid
left join MedibuddyDocsappMappings C on C.patientid= B.patient
where A.validtill>='{startDate}' and medibuddyuserid is not null""".format(startDate = startdate, endDate = enddate)
DFF3 = pd.read_sql(tt, node)
DFF3


# In[7]:


KP="""select DATE(A.createdat) as date,A.patient, medibuddyuserid as medi   from Consults A left join Patients B on A.patient =B.id
left join MedibuddyDocsappMappings C on C.patientid= A.patient
where A.createdat between '{startDate}' and '{endDate}' and valid = 1 and doctorresponsetime is not null 
and acceptedconsultation is not null and issummarized=1 AND medibuddyuserid IS NOT NULL""".format(startDate = startdate, endDate = enddate)
dfCM = pd.read_sql(KP,node)
dfCM


# In[8]:


DFF3['patientid'].nunique()


# In[9]:


dfCM['patient'].nunique()


# In[10]:


dffx=dfCM.merge(DFF3, how='inner',  left_on='medi', right_on='medibuddyuserid')


# In[11]:


dffx


# In[12]:


dffx=dffx.drop_duplicates()


# In[ ]:





# In[13]:


dffx['patientid'].nunique()


# In[14]:


G2C=dffx['patientid'].nunique()/DFF3['patientid'].nunique()


# In[15]:


G2C*100


# In[16]:


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


# In[ ]:





# In[17]:


dff=DFF2.merge(dffx, how='inner',  left_on='TAU_ID', right_on='medibuddyuserid')


# In[18]:


dff


# In[27]:


k=dff.drop_duplicates()


# In[28]:


k


# In[25]:


dff['CONTACTNO'].nunique()


# In[26]:


dff['medi'].nunique()


# In[ ]:


C2M=757/8613


# In[ ]:


C2M


# In[ ]:


G->C->M = 72904->8613->786   (Unique users)


# In[29]:


AVG_Meds_per_consultpatient=40545/4809


# In[30]:


AVG_Meds_per_consultpatient


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


ppp="""SELECT DISTINCT CorporateName,
                OrderDate AS RequestDate,
                ContractTypeName AS ContractName,
                PatientName,
                EmployeeId,
                Age,
                Gender,
                Relationship,
                EmailId,
                ContactNo,
                PackageName,
                PackageInvestigations,
                ApptCreatedDate,
                convert(date, ApptDate) AS AppointmentDate,
                convert(time(0), ApptDate) AS ApptTime,
                JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.visitType') AS VisitType,
                ProviderName,
                ProviderState,
                ProviderCity,
                ProviderLocation,
                ProviderERId AS DcId,
                mmr.RequestID AS AppointmentId,
                OrderId,
                AppointmentStatus,
                ClientCity,
                MABranch,
                StatusTakenBy,
                StatusReceivedFrom,
                StatusGivenBy,
                CASE
                    WHEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.isPaid') = 'true' THEN 'Paid'
                    WHEN JSON_VALUE(CONVERT(varchar(MAX), Properties), '$.isPaid') = 'false' THEN 'Sponsored'
                END AS PackageType,
                MedicalPendingRemark,
                MAID AS MemberId,
                RequestCreatedBy,
                CASE
                    WHEN TransactionId IS NOT NULL THEN TransactionId
                    ELSE rw.RW_TrackingNumber
                END AS ClaimReferenceNumber
FROM rpt.MediMarketRequest mmr (nolock)
LEFT JOIN
  (SELECT O_Id,
          TransactionId
   FROM Medimarket.marketplace.orderfinancial (nolock)) oft ON oft.O_ID=OrderId
LEFT JOIN
  (SELECT RW_R_Id,
          RW_TrackingNumber
   FROM Medimarket.dbo.tblRequestWallet (nolock)) rw ON rw.RW_R_Id=RequestId
WHERE mmr.IsActive= 1
  AND ContractTypeId IN (5291,
                         10124,
                         10302,
                         10303,
                         10304,
                         10386,
                         10389,
                         10401,
                         10595,
                         10845,
                         10846,
                         10847,
                         10861,
                         10949,
                         12553,
                         13193,
                         13465,
                         13623,
                         13973)
  AND ApptCreatedDate>='2021-04-01'"""

dfpp = pd.read_sql(ppp, conMB)
dfpp


# In[ ]:


dfpp.to_csv('data2.csv')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




