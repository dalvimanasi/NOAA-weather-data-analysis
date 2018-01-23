import luigi
import logging
import datetime
import json
import boto3
from botocore.client import Config
import pandas as pd
from pathlib import Path
import csv
import os

NOW = datetime.datetime.now()
TODAYSDATE = str(NOW.day).zfill(2)+str(NOW.month).zfill(2)+str(NOW.year)
TODAYSDATESTRING = str(NOW.day).zfill(2)+"/"+str(NOW.month).zfill(2)+"/"+str(NOW.year)
OUTPUTPATH = os.environ['OUTPUTPATH']+"/"
CONFIGPATH = os.environ['CONFIGPATH']+"/"
LOGPATH = os.environ['LOGPATH'] + "/"
LOGFILENAME = LOGPATH+"/"+TODAYSDATE+str(NOW.hour).zfill(2)+str(NOW.minute).zfill(2)+str(NOW.second).zfill(2)+".log"
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',filename=LOGFILENAME,datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)

CONFIGFILENAME = "configWrangle.json"
SAVEDCONFIGFILENAME = OUTPUTPATH+"/TEMP_wranglingpreprocess"+TODAYSDATE+".json"

TEMPFILE1PATH = OUTPUTPATH+"TEMP_1_"+TODAYSDATE+".csv"

def validateConfigFile(config_file):
    try:
        if config_file['state'] == "":
            logging.error("State is not defined in the config file")
            return False
        if config_file['team'] == "":
            logging.error("Team is not defined in the config file")
            return False
        if config_file['rawData'] == "":
            logging.error("Raw data bucket name not defined in the config file")
            return False
        if config_file['cleanData'] == "":
            logging.error("Clean data bucket name not defined in the config file")
            return False
        if config_file['AWSAccess'] == "":
            logging.error("AWSAccess is not defined in the config file")
            return False
        if config_file['AWSSecret'] == "":
            logging.error("AWSSecret is not defined in the config file")
            return False
        if config_file['notificationEmail'] == "":
            logging.error("notificationEmail is not defined in the config file")
            return False
        return True
    except:
        logging.error("Config File not complete. Check if all keys (state,team,AWSAccess,AWSSecret,notificationEmail,weatherStation) exist.")
        exit()

def readConfig():
    fil = open(SAVEDCONFIGFILENAME,'r')
    conf = json.load(fil)
    fil.close()
    return conf
def getRawBucketName():
    conf = readConfig()
    BUCKET_NAME = conf['RAWBUCKET']
    return BUCKET_NAME

def getCleanBucketName():
    conf = readConfig()
    BUCKET_NAME = conf['CLEANBUCKET']
    return BUCKET_NAME

def getS3Resource():
    conf = readConfig()
    S3 = boto3.resource('s3',
            aws_access_key_id= conf['AWSACCESS'],
            aws_secret_access_key=conf['AWSSECRET'],
            config=Config(signature_version='s3v4')
            )
    return S3
    
def getS3Client():
    conf = readConfig()
    CLIENT = boto3.client('s3',
            aws_access_key_id=conf['AWSACCESS'],
            aws_secret_access_key=conf['AWSSECRET'],
            config=Config(signature_version='s3v4')
        )
    return CLIENT
def fahr_to_celsius(temp_fahr):
    """Convert Fahrenheit to Celsius
    
    Return Celsius conversion of input"""
    temp_celcius = (temp_fahr - 32) * 5 / 9
    return round(temp_celcius,2)

def celcius_to_fahr(temp_celcius):
    """Convert Celcius to Farenheit
    
    Return Farenheit conversion of input"""
    temp_fahr = 9 / 5 * temp_celcius + 32
    return round(temp_fahr,2)
class UploadCleanDataToS3(luigi.Task):
    uploadCleanDataToS3 = "TEMP_uploadCleanDataSToS3.txt"
    def requires(self):
        return WrangleRawData()
    def run(self):
        logging.info("Task UploadCleanDataToS3 BEGIN")
        clean_bucketname = getCleanBucketName()
        logging.info("Reading todays cleaned file "+self.input().path)
        keyName = self.input().path.split("/")[-1]
        data = open(self.input().path,'rb')
        logging.info("Uploading todays clean file "+keyName+" to S3 bucket " + clean_bucketname+ "...")
        S3 = getS3Resource()
        S3.Bucket(clean_bucketname).put_object(Key=keyName, Body=data)
        data.close()

        logging.info("File "+keyName+" Uploaded successfully to S3 bucket " + clean_bucketname)
        
        f = self.output().open('w')
        f.write("SUCCESS")
        f.close()
        logging.info("Task UploadCleanDataToS3 ENDS")
    def output(self):
        return luigi.LocalTarget(OUTPUTPATH+self.uploadCleanDataToS3)

class WrangleRawData(luigi.Task):
    finalCleanFilename = "TEMP_WrangleFile.csv"
    def requires(self):
        return FetchRawDataFromS3()
    def run(self):
        logging.info("Task WrangleRawData BEGIN")
        logging.info("Reading todays file : "+self.input().path.split("/")[-1])

        logging.info("Beginning Data Wrangling")
        #write wrangling code
        
        #First step replace unrequired data to make the numeric columns numeric
        with open(TEMPFILE1PATH, 'w') as writefile:
            with open(self.input().path, 'r') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=',')
                count =0
                for row in spamreader:
                    count+=1
                    if count==1:
                        writefile.write(','.join(row)) 
                    if count > 1:
                        row[8]=row[8].replace('VRB','-1').replace('V','').replace('s','').replace('*','').replace('T','0.005')
                        row[10:36]=(','.join(row[10:36]).replace('VRB','-1').replace('V','').replace('s','').replace('*','').replace('T','0.005')).split(",")
                        row[38:]=(','.join(row[38:]).replace('VRB','-1').replace('V','').replace('s','').replace('*','').replace('T','0.005')).split(",")
                        if row[11]=="" and row[10]!="":
                            row[11] = str(fahr_to_celsius(float(row[10])))
                        if row[10]=="" and row[11]!="":
                            row[10] = str(celcius_to_fahr(float(row[11])))
        
                        if row[13]=="" and row[12]!="":
                            row[13] = str(fahr_to_celsius(float(row[12])))
                        if row[12]=="" and row[13]!="":
                            row[12] = str(celcius_to_fahr(float(row[13])))
                        
                        if row[15]=="" and row[14]!="":
                            row[15] = str(fahr_to_celsius(float(row[14])))
                        if row[14]=="" and row[15]!="":
                            row[14] = str(celcius_to_fahr(float(row[15])))
                        
                        if(row[12]=="" and row[13]=="" and row[10] != "" and row[14]!=""):
                            if float(row[10]) <= 65:
                                row[12] = str(float(row[10]) - (float(row[10]) - float(row[14]))/3)
                                row[13] = str(fahr_to_celsius(float(row[12])))
                        writefile.write('\n')
                        writefile.write(','.join(row))
        
        #inputfile = pd.read_csv(self.input().path,low_memory=False)
        rawFileName = self.input().path.split("/")[-1].split(".")[0]
        
        
        data_ma_num = pd.read_csv(TEMPFILE1PATH,low_memory=False)
        data_ma_num['DATE'] = pd.to_datetime(data_ma_num['DATE'])
        rpt_type = ['SOD','SOM']
        data_ma_num = data_ma_num[~data_ma_num['REPORTTPYE'].isin(rpt_type)]
        data_ma_num = data_ma_num.set_index('DATE')
        #sorting date wise
        data_ma_num.sort_index(inplace=True)
        #data_ma_num = data_ma_num.interpolate().where(data_ma_num.bfill().notnull())
        #data_ma_num['HOURLYDRYBULBTEMPF'] = data_ma_num['HOURLYDRYBULBTEMPF'].interpolate().where(data_ma_num['HOURLYDRYBULBTEMPF'].bfill().notnull())
        #data_ma_num['HOURLYDRYBULBTEMPC'] = data_ma_num['HOURLYDRYBULBTEMPC'].interpolate().where(data_ma_num['HOURLYDRYBULBTEMPC'].bfill().notnull())
        #data_ma_num['HOURLYDewPointTempF'] = data_ma_num['HOURLYDewPointTempF'].interpolate().where(data_ma_num['HOURLYDewPointTempF'].bfill().notnull())
        #data_ma_num['HOURLYDewPointTempC'] = data_ma_num['HOURLYDewPointTempC'].interpolate().where(data_ma_num['HOURLYDewPointTempC'].bfill().notnull())
        #data_ma_num['HOURLYWETBULBTEMPF'] = data_ma_num['HOURLYWETBULBTEMPF'].interpolate().where(data_ma_num['HOURLYWETBULBTEMPF'].bfill().notnull())
        #data_ma_num['HOURLYWETBULBTEMPC'] = data_ma_num['HOURLYWETBULBTEMPC'].interpolate().where(data_ma_num['HOURLYWETBULBTEMPC'].bfill().notnull())
      
        
        data_ma_num = data_ma_num.reset_index()
        datecolname = "DATE"
        A = data_ma_num.columns
        A=A[1:]
        B=[]
        removeColumnList = ['DAILYMaximumDryBulbTemp','DAILYMinimumDryBulbTemp','DAILYAverageDryBulbTemp','DAILYDeptFromNormalAverageTemp','DAILYAverageRelativeHumidity','DAILYAverageDewPointTemp','DAILYAverageWetBulbTemp','DAILYHeatingDegreeDays','DAILYCoolingDegreeDays','DAILYWeather','DAILYPrecip','DAILYSnowfall','DAILYSnowDepth','DAILYAverageStationPressure','DAILYAverageSeaLevelPressure','DAILYAverageWindSpeed','DAILYPeakWindSpeed','PeakWindDirection','DAILYSustainedWindSpeed','DAILYSustainedWindDirection','MonthlyMaximumTemp','MonthlyMinimumTemp','MonthlyMeanTemp','MonthlyAverageRH','MonthlyDewpointTemp','MonthlyWetBulbTemp','MonthlyAvgHeatingDegreeDays','MonthlyAvgCoolingDegreeDays','MonthlyStationPressure','MonthlySeaLevelPressure','MonthlyAverageWindSpeed','MonthlyTotalSnowfall','MonthlyDeptFromNormalMaximumTemp','MonthlyDeptFromNormalMinimumTemp','MonthlyDeptFromNormalAverageTemp','MonthlyDeptFromNormalPrecip','MonthlyTotalLiquidPrecip','MonthlyGreatestPrecip','MonthlyGreatestPrecipDate','MonthlyGreatestSnowfall','MonthlyGreatestSnowfallDate','MonthlyGreatestSnowDepth','MonthlyGreatestSnowDepthDate','MonthlyDaysWithGT90Temp','MonthlyDaysWithLT32Temp','MonthlyDaysWithGT32Temp','MonthlyDaysWithLT0Temp','MonthlyDaysWithGT001Precip','MonthlyDaysWithGT010Precip','MonthlyDaysWithGT1Snow','MonthlyMaxSeaLevelPressureValue','MonthlyMaxSeaLevelPressureDate','MonthlyMaxSeaLevelPressureTime','MonthlyMinSeaLevelPressureValue','MonthlyMinSeaLevelPressureDate','MonthlyMinSeaLevelPressureTime','MonthlyTotalHeatingDegreeDays','MonthlyTotalCoolingDegreeDays','MonthlyDeptFromNormalHeatingDD','MonthlyDeptFromNormalCoolingDD','MonthlyTotalSeasonToDateHeatingDD','MonthlyTotalSeasonToDateCoolingDD']
        for d in data_ma_num.columns:
            if d not in removeColumnList:
                if d!=datecolname:
                    B.append(d)
                if d=="LONGITUDE":
                    B.append(datecolname)
        data_ma_num = data_ma_num[B]
        
        logging.info("Data Wrangling Complete... Saving new file")
        self.finalCleanFilename = rawFileName+"_clean.csv"
        data_ma_num.to_csv(OUTPUTPATH+self.finalCleanFilename,index=False)
        logging.info("New file "+OUTPUTPATH+self.finalCleanFilename+" created")
        logging.info("Task WrangleRawData ENDS")
    def output(self):
         return luigi.LocalTarget(OUTPUTPATH+self.finalCleanFilename)

class FetchRawDataFromS3(luigi.Task):
    s3FileName = "TEMP_FetchRawDataFromS3.csv"
    def requires(self):
        return ReadConfigFilePreProcess()
    def run(self):
        logging.info("Task FetchRawDataFromS3 BEGIN")
        CLIENT = getS3Client()
        paginator = CLIENT.get_paginator('list_objects')
        rawBucketName = getRawBucketName()
        cleanBucketName = getCleanBucketName()
        # Create a PageIterator from the Paginator
        raw_page_iterator = paginator.paginate(Bucket=rawBucketName)
        clean_page_iterator = paginator.paginate(Bucket=cleanBucketName)
        
        todays_date = datetime.datetime.strptime(TODAYSDATE, '%d%m%Y')
        
        logging.info("S3 connection successful. Searching for latest file.")
        for page in clean_page_iterator:
            for contentsarrayelem in page['Contents']:
                if str(contentsarrayelem.get('Key')).endswith("clean.csv"):
                    curr_date = datetime.datetime.strptime(contentsarrayelem['Key'].split("_")[1], '%d%m%Y')
                    if curr_date == todays_date:
                        logging.error("Clean Data for "+TODAYSDATESTRING+" already exists on S3 bucket "+cleanBucketName+" with key = "+contentsarrayelem['Key'])
                        raise CustomDataIngestionException("Clean Data for "+TODAYSDATESTRING+" already exists on S3 bucket "+cleanBucketName+" with key = "+contentsarrayelem['Key']+"\nWill Not attempt wrangling.")
                        break
                
        
                
        rawFileKey = ""
        for page in raw_page_iterator:
            for contentsarrayelem in page['Contents']:
                if not str(contentsarrayelem.get('Key')).endswith("clean.csv"):
                    curr_date = datetime.datetime.strptime(contentsarrayelem['Key'].split("_")[1], '%d%m%Y')
                    if curr_date == todays_date:
                        logging.error("Raw Data for "+TODAYSDATESTRING+" found "+cleanBucketName+" with key = "+contentsarrayelem['Key'])
                        rawFileKey = contentsarrayelem['Key']
                        break
        if rawFileKey == "":
            logging.error("Raw File for todays date "+TODAYSDATESTRING+" not foud on S3. Please run Data Ingestion first to upload todays data.")
            raise CustomDataIngestionException("Raw File for todays date "+TODAYSDATESTRING+" not foud on S3")
        else:
            checkFile = Path(OUTPUTPATH+rawFileKey)
            if not checkFile.is_file():
                logging.info("Downloading the file : "+rawFileKey)
                CLIENT.download_file(rawBucketName, Filename=OUTPUTPATH+rawFileKey,Key=rawFileKey)
                logging.info(rawFileKey+" succeessfully downloaded")
            else:
                logging.info("File already exists locally path : "+rawFileKey+".. will not download from S3.")
            self.s3FileName = rawFileKey
        logging.info("Task FetchRawDataFromS3 ENDS")
    def output(self):
        return luigi.LocalTarget(OUTPUTPATH+self.s3FileName)


class ReadConfigFilePreProcess(luigi.Task):
    def run(self):
        logging.info("Task ReadConfigFilePreProcess BEGIN")
        logging.info("Reading the config file - "+CONFIGFILENAME)
        GLOBALPARAMS = {
            'STATE':"",
            'AWSACCESS': "",
            'AWSSECRET':"",
            'TEAMNO':"",
            'RAWBUCKET':"",
            'CLEANBUCKET':"",
            'NOTIFYEMAIL':""
        }
        try:
            config_file_wrangle = open(CONFIGPATH+CONFIGFILENAME)
            config_file = json.load(config_file_wrangle)
            if not validateConfigFile(config_file):
                raise
            #Set parameters derived from file
            GLOBALPARAMS['STATE'] = config_file['state']
            GLOBALPARAMS['RAWBUCKET'] = config_file['rawData']
            GLOBALPARAMS['CLEANBUCKET'] = config_file['cleanData']
            GLOBALPARAMS['AWSACCESS'] = config_file['AWSAccess']
            GLOBALPARAMS['AWSSECRET'] = config_file['AWSSecret']
            GLOBALPARAMS['TEAMNO'] = config_file['team']
        except:
            logging.error("Error opening /parsing the config file "+CONFIGFILENAME + "\nCheck if the file" +CONFIGFILENAME+ " is well formed")
            config_file_wrangle.close()
            raise
        finally:
            config_file_wrangle.close()
        config_file_wrangle.close()
        logging.info("Config File "+CONFIGFILENAME+" valid.")
        
        with self.output().open("w") as fp:
            json.dump(GLOBALPARAMS, fp)
        
        logging.info("Data Ingestion Preprocessing completed."+ SAVEDCONFIGFILENAME + " written. This will be deleted after the job is complete.")
        logging.info("Task ReadConfigFilePreProcess ENDS")
        
    def output(self):
        return luigi.LocalTarget(SAVEDCONFIGFILENAME)


class CustomDataIngestionException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)





