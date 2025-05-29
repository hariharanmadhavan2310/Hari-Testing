delete from AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL where injectday >= current_date-4;

insert into AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL 
select 
to_date(a.injectdate) AS INJECTDAY,
b.ipaddress as ipaddress,
SITEPLATFORMVALUE,
ENDUSERLATITUDEVALUE,
ENDUSERLONGITUDEVALUE,
UNIQUEUSERID,
a.FILENAME,
a.INJECTDATE,
a.VERSION,
a.REQUESTID,
a.EVENT,
a.STATUS,
a.SRCREQUESTID,
a.IMPRESSIONID,
a.ADID,
a.CAMPAIGNID,
a.SRCURLHASH,
a.SITEID,
a.COUNTRYID,
a.DEVICEID,
a.DEVICEMANUFACTURERID,
a.DEVICEMODELID,
a.DEVICEOSID,
a.USERAGENT,
a.IPADDRESS as postimpression_ip,
a.XFORWARDEDFOR,
a.TIMEVALUE,
a.EVENTTIME,
a.AUCTION_PRICE,
a.AUCTION_CURRENCY,
a.AUCTION_BID_ID,
a.AUCTION_ID,
a.AUCTION_IMP_ID,
a.EXCHANGEID,
a.COUNTRYCARRIERID,
a.SELECTEDSITECATEGORYID,
a.URLVERSION,
a.INVENTORYSOURCE,
a.BIDPRICE_TO_EXCHANGE,
a.INTERNAL_MAX_BID,
a.ADVERTISER_BID,
a.BIDDERMODELID,
a.MARKETPLACE_ID,
a.SLOTID,
a.DEVICEBROWSERID,
a.CPA_GOAL,
a.SUPPLY_SOURCE_TYPE,
a.EXT_SUPPLY_ATTR_INTERNAL_ID,
a.CONNECTIONTYPEID,
a.ADV_INC_ID,
a.PUB_INC_ID,
a.DEVICETYPE,
a.BIDFLOOR,
a.EXCHANGEUSERID,
a.EXTERNALSITEAPPID,
a.STATEID,
a.CITYID,
a.ADPOSITIONID,
a.CHANNELID,
a.EXPERIMENTID,
a.REQUESTMODE,
a.TARGETABLEDOMAIN,
a.URLEXTRAPARAMETERS_P,
a.URLEXTRAPARAMETERS_ABID,
a.URLEXTRAPARAMETERS_IID,
a.URLEXTRAPARAMETERS_KDOM,
a.URLEXTRAPARAMETERS_AIMPID,
a.URLEXTRAPARAMETERS_AID,
a.URLEXTRAPARAMETERS_BP,
a.ID,
a.TEVENT,
a.URLEXTRAPARAMETERS,
a.RAW_LOG,
a.LINEITEMID,
a.PARENTIDADV,
a.PARENTIDPUB,
a.SLOTVIEWABILITY,
a.SSPID,
a.IS_CONVERSION_TRACKED,
a.GDPR,
a.CONSENT,
a.PRODID,
a.CB_CHRM,
a.BANNERID,
a.IS_NEW_USER,
a.AAID,
a.EXTMONETARY,
a.REQFORMATID,
a.AUCTION_LOSS_CODE,
a.DETECTEDCOUNTRYID,
a.INTERNALSITESLOTID,
a.EXTERNALSITESLOTID,
a.CREATIVEID,
a.ISPOSTBACKCONVERSION,
a.SEG,
a.ADSERVINGCONVREQIDS,
a.POSTDATA,
a.GETDATA,
a.ADTAXONOMYIDS,
a.PUBREFERER,
a.HTML5LANDINGPAGECLICKURL,
a.SEGS,
a.SURVEYRESPONSES,
a.GENERICEVENTTYPE,
a.CNVEVENTS,
a.KRITTERUSERID,
a.BUYERUID,
a.TERMINATIONREASON_NAME,
a.DETECTED_COUNTRY_ID,
a.DETECTED_STATE_ID,
a.DETECTED_CITY_ID,
a.DETECTED_DEVICETYPE_ID,
a.DETECTED_COUNTRY_CARRIER_ID,
a.DETECTED_CONNECTION_TYPE_ID,
a.DETECTED_OS_ID,
a.DETECTED_BROWSER_ID,
a.DETECTED_MANUFACTURER_ID,
a.PRODUCTID,
a.BRAND,
a.ASKONE_RESPONSE,
a.TEVENTTYPE,
a.ORGNAME,
'' as ad_type, 
'' , 
parse_ip(b.IPADDRESS,'INET',1):ipv4 as INT_IP,
'',
'',
'',
'',
'',
'',
'',
b.zip,
Null /*from AROSCOP_EXT.SCHEMA_AROSCOP_EXT.AROSCOP_POSTIMPRESSION_LOG_DETAILS a left join AROSCOP_EXT.SCHEMA_AROSCOP_EXT.AROSCOP_ADSERVING_LOG_DETAILS b */ from sciera_dataset.public.AROSCOP_POSTIMPRESSION_LOG_DETAILS a left join sciera_dataset.public.AROSCOP_ADSERVING_LOG_DETAILS b on a.SRCREQUESTID=b.requestid where b.advertiserid = '010ab663-b671-1a01-8f58-74ca3b000217' and date(a.injectdate)>=current_date-4;
 
update AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL a set a.ad_type=iff(b.creative_format='Video','VIDEO','DISPLAY') from AROSCOP_EXT.SCHEMA_AROSCOP_EXT.AROSCOP_CREATIVEs b 
where a.creativeid=b.creative_id and (a.ad_type is NULL or a.ad_type='');
 
UPDATE AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL IP
SET
IP.SCIERA_TYPE=MI.SCIERA_TYPE,
IP.STD_NAME=MI.STD_NAME,
IP.MAXMIND=MI.MAXMIND_NAME,
IP.IP2=MI.IP2_NAME,
IP.IP2_DOMAIN=MI.IP2_DOMAIN,
IP.MAXMIND_CT=MI.MAXMIND_CONNECTION_TYPE,
IP.IP2_CT=MI.IP2_CONNECTION_TYPE
FROM
"LIFE_V3"."RESOURCE"."MAXIP2_BLOCKS" MI
WHERE
 LENGTH(IPV4_INTEGER_FROM)<=10 AND LENGTH(IPV4_INTEGER_TO)<=10 AND
TO_NUMBER(IPV4_INTEGER_FROM,38,0)<=INT_IP AND INT_IP<= TO_NUMBER(IPV4_INTEGER_TO,38,0) and (ip.std_name is null or ip.std_name='') and 
INJECTDAY >= current_date-4;

update AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL set ip_int_v6 = AROSCOP_DM_COX.ANALYSIS.IPV6_TO_LONG(IPADDRESS) where 
(STD_NAME IS NULL or std_name='') AND IPADDRESS LIKE '%:%' AND IPADDRESS NOT LIKE '%::%' AND
LENGTH(SPLIT_PART(IPADDRESS, ':', -1))>0 and parse_ip(ipaddress, 'INET'):family=6 and 
INJECTDAY >= current_date-4;


/*Luke table bins ipv6*/

UPDATE AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL IP
SET
IP.SCIERA_TYPE=MI.SCIERA_TYPE,
IP.STD_NAME=MI.final_STD_NAME,
IP.MAXMIND=MI.MAXMIND,
IP.IP2=MI.IP2_NAME,
IP.IP2_DOMAIN=MI.IP2_DOMAIN,
IP.MAXMIND_CT=MI.MAXMIND_CONNECTION_TYPE,
IP.IP2_CT=MI.IP2_CONNECTION_TYPE
FROM
aroscop_publish.poc.postimpression_ip6_MaxIp2 MI
WHERE
 ip.ip_int_v6 =mi.ip_int_v6 and 
INJECTDAY >= current_date-4;

update AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL set std_name='' where std_name is null and 
INJECTDAY >= current_date-4;

/*update AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL a set Zip5=LPAD(REGEXP_SUBSTR(name, '\\d{4,5}'), 5, '0') from AROSCOP_EXT.SCHEMA_AROSCOP_EXT.AROSCOP_CAMPAIGn b where a.campaignid=b.id and (zip5 is null or zip5='')*/

create or replace table AROSCOP_DM_COX.REPORTING.ZIP5_REPORT as 
select injectday as date,campaignid,zip5,ad_type,creativeid,count(distinct impressionid) as total_impressions from AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL group by 1,2,3,4,5;

alter table AROSCOP_DM_COX.REPORTING.ZIP5_REPORT add column TOTAL_CLICKS NUMBER(18,0), SUB_IMPRESSIONS NUMBER(18,0), SUB_CLICKS NUMBER(18,0), NONSUB_IMPRESSIONS NUMBER(18,0), NONSUB_CLICKS NUMBER(18,0);

update AROSCOP_DM_COX.REPORTING.ZIP5_REPORT a set TOTAL_CLICKS = (select count(distinct impressionid) from AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL b 
where a.date=b.injectday and a.zip5=b.zip5 and a.ad_type=b.ad_type and a.creativeid=b.creativeid and event=3);

update AROSCOP_DM_COX.REPORTING.ZIP5_REPORT a set SUB_CLICKS = (select count(distinct impressionid) from AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL b 
where a.date=b.injectday and a.zip5=b.zip5 and a.ad_type=b.ad_type and a.creativeid=b.creativeid and event=3 and STD_NAME='COX');

update AROSCOP_DM_COX.REPORTING.ZIP5_REPORT a set NONSUB_CLICKS = (select count(distinct impressionid) from AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL b 
where a.date=b.injectday and a.zip5=b.zip5 and a.ad_type=b.ad_type and a.creativeid=b.creativeid and event=3 and (STD_NAME != 'COX' or STD_NAME is null));

update AROSCOP_DM_COX.REPORTING.ZIP5_REPORT a set SUB_IMPRESSIONS = (select count(distinct impressionid) from AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL b 
where a.date=b.injectday and a.zip5=b.zip5 and a.ad_type=b.ad_type and a.creativeid=b.creativeid and STD_NAME='COX');

update AROSCOP_DM_COX.REPORTING.ZIP5_REPORT a set NONSUB_IMPRESSIONS = (select count(distinct impressionid) from AROSCOP_DM_COX.REPORTING.POSTIMPRESSION_DETAIL b 
where a.date=b.injectday and a.zip5=b.zip5 and a.ad_type=b.ad_type and a.creativeid=b.creativeid and (STD_NAME != 'COX' or STD_NAME is null)); 

create or replace table AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 as 
SELECT DATE,campaign_id as orig_campaign_id,campaign_name as orig_campaign_name,LPAD(REGEXP_SUBSTR(CAMPAIGN_NAME, '\\d{4,5}'), 5, '0') as Zip5, CASE WHEN DATE < '2024-10-15' THEN  1 ELSE 2 END AS campaign_id,substr(campaign_name,1,charindex('_BD',campaign_name,1)-1) as campaign_name,1 as placement_id,substr(creative_name,1,charindex('~V1',creative_name,1)-1) as placement_name,creative_id,creative_name,sum(total_csc) as impressions,sum(total_click) as clicks,sum(total_conversion) as conversion,sum(TOTAL_START_VIDEO) as video_views,sum(total_revenue) as media_cost,sum(TOTAL_FIRST_QUARTILE_VIDEO) as video_first_quartile_completions_number,sum(TOTAL_MIDPOINT_VIDEO) as video_midpoints_number,sum(TOTAL_COMPLETE_VIDEO) as video_completions, sum(TOTAL_THIRD_QUARTILE_VIDEO) as video_third_quartile_completions_number, cast(current_timestamp as TIMESTAMP_TZ) as sciera_synced_timestamp  
FROM AROSCOP_DM_COX.REPORTING.ALL_DIMENSION_DAILY WHERE (DATE >= '2024-05-21' AND UPPER(CREATIVE_NAME) NOT LIKE '%TEST%') OR DATE >= '2024-10-15' GROUP BY 1,2,3,4,5,6,7,8,9,10;

UPDATE AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 SET PLACEMENT_ID=2 WHERE UPPER(PLACEMENT_NAME)='LOB~RESI_CU~ALL_TAC~AWA_PB~SCIERA_DA~NONE_SA~NCCC_TG~PRO_AS~DCM_FM~VID_DT~DM_MA~MULT_FS~COLV_VN';

alter table AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 add column SUB_IMPRESSIONS NUMBER(18,0), SUB_CLICKS NUMBER(18,0), NONSUB_IMPRESSIONS NUMBER(18,0), NONSUB_CLICKS NUMBER(18,0);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 a 
set a.SUB_IMPRESSIONS=b.SUB_IMPRESSIONS, a.sub_clicks=b.sub_clicks, a.NONSUB_IMPRESSIONS=b.NONSUB_IMPRESSIONS,a.NONSUB_CLICKS=b.NONSUB_CLICKS 
from AROSCOP_DM_COX.REPORTING.ZIP5_REPORT b
where a.date=b.date and a.zip5=b.zip5 and a.creative_id=b.creativeid;

alter table AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 add column total_IMPRESSIONS NUMBER(18,0);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 a 
set a.total_IMPRESSIONS=b.total_IMPRESSIONS from AROSCOP_DM_COX.REPORTING.ZIP5_REPORT b
where a.date=b.date and a.zip5=b.zip5 and a.creative_id=b.creativeid;

alter table AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 add column total_clicks NUMBER(18,0);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 a 
set a.total_clicks=b.total_clicks from AROSCOP_DM_COX.REPORTING.ZIP5_REPORT b
where a.date=b.date and a.zip5=b.zip5 and a.creative_id=b.creativeid;

alter table AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 add column final_SUB_IMPRESSIONS NUMBER(18,0), final_SUB_CLICKS NUMBER(18,0), final_NONSUB_IMPRESSIONS NUMBER(18,0), final_NONSUB_CLICKS NUMBER(18,0);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 
set final_SUB_IMPRESSIONS = zeroifnull(SUB_IMPRESSIONS)
where zeroifnull(impressions)>=zeroifnull(total_impressions);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 
set final_SUB_IMPRESSIONS = cast(SUB_IMPRESSIONS*(impressions/total_impressions) as integer)
where zeroifnull(impressions)<zeroifnull(total_impressions);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 
set final_NONSUB_IMPRESSIONS = zeroifnull(NONSUB_IMPRESSIONS)
where zeroifnull(impressions)>=zeroifnull(total_impressions);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 
set final_NONSUB_IMPRESSIONS = cast(NONSUB_IMPRESSIONS*(impressions/total_impressions) as integer)
where zeroifnull(impressions)<zeroifnull(total_impressions);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 
set final_SUB_CLICKS=zeroifnull(SUB_CLICKS) ,final_NONSUB_CLICKS=zeroifnull(NONSUB_CLICKS)
where zeroifnull(clicks)>=zeroifnull(total_clicks);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 
set final_SUB_CLICKS = cast(SUB_CLICKS*(clicks/total_clicks) as integer) , final_NONSUB_CLICKS = cast(NONSUB_CLICKS*(clicks/total_clicks) as integer) 
where zeroifnull(clicks)<zeroifnull(total_clicks);

alter table AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 add column final_NONSUB_IMPRESSIONS_CLEAN NUMBER(18,0), final_NONSUB_CLICKS_CLEAN NUMBER(18,0);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5  
set final_NONSUB_IMPRESSIONS_CLEAN=impressions-final_SUB_IMPRESSIONS, 
final_NONSUB_CLICKS_CLEAN=clicks-final_SUB_CLICKS;

alter table AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 add column dcm_impressions number(38,0), scale_factor float, DCM_MEDIA_COST float, CREATIVE_FORMAT
VARCHAR(16777216);

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 a set a.scale_factor=b.scale_factor, a.creative_format=b.creative_format
from AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT b 
where a.creative_id=b.creative_id and a.date=b.date;

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5  set dcm_impressions=impressions*scale_factor;

update AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 set dcm_media_cost=iff(creative_format='Video',(dcm_impressions*20)/1000,(dcm_impressions*6)/1000);

COPY INTO '@"SCIERA_SUBSYSTEM"."REALWATCH"."SIS_SKYCTEL"/Production/AROSCOP_DM_COX/SCIERA_DAILY_REPORT_ZIP5/<<file_name1>>' from (select date,campaign_id,campaign_name,placement_id,placement_name,creative_id,creative_name,zip5,impressions,clicks,dcm_media_cost as media_cost,final_sub_impressions,final_sub_clicks,final_nonsub_impressions_clean,final_nonsub_clicks_clean,sciera_synced_timestamp 
from AROSCOP_DM_COX.REPORTING.SCIERA_DAILY_REPORT_ZIP5 where date <= current_date-2 AND REGEXP_LIKE(ZIP5, '\\d{5}')  ) file_format=( TYPE = CSV  FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'  COMPRESSION ='NONE'  field_delimiter = '|' skip_header = 0 NULL_IF=('') 
empty_field_as_null = false ) single=true HEADER = true OVERWRITE = TRUE MAX_FILE_SIZE = 5000000000 ;
