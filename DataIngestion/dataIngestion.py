#Organzing Imports

import datetime
import luigi
import logging
import boto3
from botocore.client import Config
import json
import pandas as pd
import requests
import io
from pathlib import Path
import os

#Declaring globa variables

MAXINT = 9223372036854775807

NOW = datetime.datetime.now()
TODAYSDATE = str(NOW.day).zfill(2)+str(NOW.month).zfill(2)+str(NOW.year)
TODAYSDATESTRING = str(NOW.day).zfill(2)+"/"+str(NOW.month).zfill(2)+"/"+str(NOW.year)
OUTPUTPATH = os.environ['OUTPUTPATH']+"/"
CONFIGPATH = os.environ['CONFIGPATH']+"/"
LOGFILENAME = os.environ['LOGPATH']+"/"+TODAYSDATE+str(NOW.hour).zfill(2)+str(NOW.minute).zfill(2)+str(NOW.second).zfill(2)+".log"
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',filename=LOGFILENAME,datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)

S3="S3 Resource"
CLIENT = "S3 Client"
BUCKET_NAME = "Bucket Name"

CONFIGFILENAME = CONFIGPATH+"config.json"
INITIALCONFIGFILENAME = CONFIGPATH+"configInitial.json"

SAVEDCONFIGFILENAME = OUTPUTPATH+"TEMP_raw_datapreprocess"+TODAYSDATE+".json"


def validateConfigFile(config_file):
    try:
        if config_file['state'] == "":
            logging.error("state is not defined in the config file")
            return False
        if str(config_file['team']) == "":
            logging.error("team is not defined in the config file")
            return False
        if config_file['link'] == "":
            logging.error("link is not defined in the config file")
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
        logging.error("Config File not complete or unparseable. Check if all keys (state,team,AWSAccess,AWSSecret,notificationEmail,weatherStation) exist.")

def validateInitalConfigFile(config_file_initial):
    try:
        if config_file_initial['state'] == "":
            logging.error("state is not defined in the initial config file")
            return False
        if str(config_file_initial['team'])== "":
            logging.error("team is not defined in the initial config file")
            return False
        if type(config_file_initial['link']) is list: 
            if len(config_file_initial['link']) >= 1:
                for f in config_file_initial['link']:
                    if f == "":
                        logging.error("one of the links are empty in the initial config file")
                        return False
            else:
                logging.error("link is empty in the initial config file")
                return False
        else:
            logging.error("the element link must be a list for configInitial.json")
            return False
        if config_file_initial['AWSAccess'] == "":
            logging.error("AWSAccess is not defined in the initial config file")
            return False
        if config_file_initial['AWSSecret'] == "":
            logging.error("AWSSecret is not defined in the initial config file")
            return False
        if config_file_initial['notificationEmail'] == "":
            logging.error("notificationEmail is not defined in the initial config file")
            return False
        return True
    except:
        logging.error("Initial Config File not complete or unparsable. Check if all keys (state,team,AWSAccess,AWSSecret,notificationEmail,weatherStation) exist.")


def readConfig():
    fil = open(SAVEDCONFIGFILENAME,'r')
    conf = json.load(fil)
    fil.close()
    return conf
def getBucketName():
    conf = readConfig()
    BUCKET_NAME = "Team" + str(conf['TEAMNO']) + conf['STATE'] +"Assignment1"
    return BUCKET_NAME
class UploadRawDataToS3(luigi.Task):
    uploadRawDataToS3 = "TEMP_uploadRawDataSToS3.txt"
    def requires(self):
        return MergeFiles()
    def run(self):
        conf = {}
        conf = readConfig()
        S3 = boto3.resource('s3',
            aws_access_key_id= conf['AWSACCESS'],
            aws_secret_access_key=conf['AWSSECRET'],
            config=Config(signature_version='s3v4')
            )
        
        BUCKET_NAME = getBucketName()
        logging.info("Reading todays prepared file "+self.input().path)
        data = open(self.input().path,'rb')
        keyName = self.input().path.split("/")[-1]
        logging.info("Uploading todays raw data file "+keyName+" to S3 bucket " + BUCKET_NAME+ "...")
        S3.Bucket(BUCKET_NAME).put_object(Key=keyName, Body=data)
        data.close()
        
        logging.info("File "+keyName+" Uploaded successfully to S3 bucket " + BUCKET_NAME)
        
        f = self.output().open('w')
        f.write("SUCCESS")
        f.close()
    def output(self):
        return luigi.LocalTarget(OUTPUTPATH+self.uploadRawDataToS3)

class MergeFiles(luigi.Task):
    finalOutputFile = "TEST_XXX.csv"
    def requires(self):
        yield RawDataFetchFromNOAA()
        yield RawDataFetchFromS3()
    def run(self):
        frames=[]
        logging.info("Merging file from history file from S3 bucket and latest file from NOAA")
        for ip in self.input():
            frames.append(pd.read_csv(ip.path,dtype='object'))
        res = pd.concat(frames)
        logging.info("Merging complete")
        weatherStation = res['STATION'].unique()[0].replace(":","_")
        res['DATE'] = res['DATE'].astype('datetime64[ns]')
        dedupdf = res.drop_duplicates(['DATE'], keep='last')       
        #sortedData = dedupdf.sort_values(by=('DATE'), ascending=True)
        conf = readConfig()
        self.finalOutputFile = conf['STATE']+"_"+TODAYSDATE+"_"+weatherStation+".csv"
        logging.info("Writing merged data to "+self.finalOutputFile)
        dedupdf.to_csv(OUTPUTPATH+self.finalOutputFile,index=False)
        logging.info("erged Data written to "+self.finalOutputFile+" successfully")
    def output(self):
        return luigi.LocalTarget(OUTPUTPATH+self.finalOutputFile)

class RawDataFetchFromNOAA(luigi.Task):
    
    noaaFileName = "TEMP_RawDataFromNOAA_"+TODAYSDATE+".csv"
    
    def requires(self):
        return DataPreprocess()
    def run(self):
        
        print(S3)
        with self.input().open("r") as file:
            fileJson = json.load(file)
            frames=[]
            if fileJson['USEINITALCONFIG'] == True:
                logging.info("Since bucket is empty. Reading from links present in " + INITIALCONFIGFILENAME)
                for httplink in fileJson['NOAALINKS']:
                    logging.info("Attempting to read from "+httplink)
                    s=requests.get(httplink).content
                    readCSVDataFrame=pd.read_csv(io.StringIO(s.decode('utf-8')),low_memory=False)
                    frames.append(readCSVDataFrame)
                    logging.info("Reading from "+httplink+" successfull")
                
                httplink = fileJson['NOAALINK']
                logging.info("Attempting to read from "+httplink+" from the config file "+CONFIGFILENAME)
                s=requests.get(httplink).content
                readCSVDataFrame=pd.read_csv(io.StringIO(s.decode('utf-8')),low_memory=False)
                frames.append(readCSVDataFrame)
                logging.info("Reading from "+httplink+" successfull")

            else:
                httplink = fileJson['NOAALINK'] 
                logging.info("Attempting to read from "+httplink+" from the config file "+CONFIGFILENAME)
                s=requests.get(httplink).content
                readCSVDataFrame=pd.read_csv(io.StringIO(s.decode('utf-8')),low_memory=False)
                frames.append(readCSVDataFrame)
                logging.info("Reading from "+httplink+" successfull")
            concatedDataframe = pd.concat(frames)
            logging.info("Saving Data fetched from NOAA APi to file : "+self.noaaFileName)
            concatedDataframe.to_csv(self.output().path,index=False)
    def output(self):
        return luigi.LocalTarget(OUTPUTPATH+self.noaaFileName)

class RawDataFetchFromS3(luigi.Task):
    
    s3FileName = "TEMP_NoFileOnS3.tmp_"+TODAYSDATE+".csv"
    
    def requires(self):
        return DataPreprocess()
    def run(self):
        with self.input().open("r") as file:
            fileJson = json.load(file)
            
            if fileJson['USEINITALCONFIG'] == True:
                count = 0
                for httplink in fileJson['NOAALINKS']:
                    #print(httplink)
                    logging.info("Since bucket is empty... Reading from first link : "+httplink+" to get header information")
                    count+=1
                    if count <= 1:
                        s=requests.get(httplink).content
                        readCSVDataFrame=pd.read_csv(io.StringIO(s.decode('utf-8')),low_memory=False)
                        #weatherStation = readCSVDataFrame['STATION'].unique()[0].replace(":","_")
                        #self.s3FileName = fileJson['STATE']+"_"+str(NOW.day)+str(NOW.month)+str(NOW.year)+"_"+weatherStation+".csv"
                        cols = readCSVDataFrame.columns.values
                        with open(self.output().path,'w') as f:
                            count = 0
                            for col in cols:
                                count+=1
                                if count < len(cols):
                                    f.write(col+",")
                                else:
                                    f.write(col)
            else:
                logging.info("Attempting connection with S3")
                CLIENT = boto3.client('s3',
                    aws_access_key_id=fileJson['AWSACCESS'],
                    aws_secret_access_key=fileJson['AWSSECRET'],
                    config=Config(signature_version='s3v4')
                    )
                # Create a reusable Paginator
                paginator = CLIENT.get_paginator('list_objects')
                BUCKET_NAME = getBucketName()
                # Create a PageIterator from the Paginator
                page_iterator = paginator.paginate(Bucket=BUCKET_NAME)
                
                todays_date = datetime.datetime.strptime(TODAYSDATE, '%d%m%Y')
                downloadKey = ""
                downloadDateDiff = MAXINT
                logging.info("S3 connection successful. Searching for latest file.")
                for page in page_iterator:
                    for contentsarrayelem in page['Contents']:
                        if not str(contentsarrayelem.get('Key')).endswith("clean.csv"):
                            curr_date = datetime.datetime.strptime(contentsarrayelem['Key'].split("_")[1], '%d%m%Y')
                            if curr_date == todays_date:
                                logging.error("Key = "+contentsarrayelem['Key']+ " found on S3 bucket = "+BUCKET_NAME+"\n Data for todays date "+TODAYSDATESTRING+" already processed")
                                
                                #checking local file system for this file. If doesn't exist then download
                                todays_file = Path(OUTPUTPATH+contentsarrayelem['Key'])
                                if not todays_file.is_file():
                                    logging.info("File "+contentsarrayelem['Key']+" doesn't exist locally. Downloading from S3 bucket "+BUCKET_NAME+" for running analysis.")
                                    CLIENT.download_file(BUCKET_NAME, Filename=OUTPUTPATH+contentsarrayelem['Key'],Key=contentsarrayelem['Key'])
                                    logging.info("File "+contentsarrayelem['Key']+" downloaded from S3 bucket "+BUCKET_NAME)
                                raise CustomDataIngestionException("Key = "+contentsarrayelem['Key']+ " found on S3 bucket = "+BUCKET_NAME+"\n Data for todays date "+TODAYSDATESTRING+" already processed")
                            elif int(str(todays_date-curr_date).split()[0]) < 0:
                                logging.error("Key = "+contentsarrayelem['Key']+ " greater than todays date "+TODAYSDATESTRING+" found on S3 bucket "+BUCKET_NAME+"\nPlease delete this file from S3 or try again after "+str(curr_date))
                                raise CustomDataIngestionException("Key = "+contentsarrayelem['Key']+ " greater than todays date "+TODAYSDATESTRING+" found on S3 bucket "+BUCKET_NAME+"\nPlease delete this file from S3 or try again after "+str(curr_date))
                            elif int(str(todays_date-curr_date).split()[0]) < downloadDateDiff:
                                downloadKey = contentsarrayelem['Key']
                            
                #Download latest file
                if downloadKey == "":
                    logging.error("Could not determine latest file to download, key is empty")
                    raise CustomDataIngestionException("Could not determine latest file to download, key is empty")
                checkFile = Path(OUTPUTPATH+downloadKey)
                if not checkFile.is_file():
                    logging.info("Latest file on S3 : "+downloadKey)
                    logging.info("Downloading the file : "+downloadKey)
                    CLIENT.download_file(BUCKET_NAME, Filename=OUTPUTPATH+downloadKey,Key=downloadKey)
                    logging.info(downloadKey+" succeessfully downloaded")
                else:
                    logging.info("Latest file present filename : "+downloadKey+" , will not attempt to download from S3")
                self.s3FileName = downloadKey

    def output(self):
        return luigi.LocalTarget(OUTPUTPATH+self.s3FileName)

class DataPreprocess(luigi.Task):
    
    
    def run(self):
        logging.info("Data Preprocessing started")
        logging.info("Reading the config file - "+CONFIGFILENAME)
        config_file={}
        self._dataprepstat = True
        GLOBALPARAMS = {
            'STATE':"",
            'NOAALINK':"",
            'NOAALINKS':[],
            'AWSACCESS': "",
            'AWSSECRET':"",
            'TEAMNO':"",
            'BUCKET_NAME':"",
            'NOTIFYEMAIL':"",
            'USEINITALCONFIG': False
        }
                    
        try:
            config_file_raw = open(CONFIGFILENAME)
            config_file = json.load(config_file_raw)
            if not validateConfigFile(config_file):
                raise
            #Set parameters derived from file
            GLOBALPARAMS['STATE'] = config_file['state']
            GLOBALPARAMS['NOAALINK'] = config_file['link']
            GLOBALPARAMS['AWSACCESS'] = config_file['AWSAccess']
            GLOBALPARAMS['AWSSECRET'] = config_file['AWSSecret']
            GLOBALPARAMS['TEAMNO'] = config_file['team']
            GLOBALPARAMS['NOTIFYEMAIL'] = config_file['notificationEmail']
        except:
            logging.error("Error opening /parsing the config file "+CONFIGFILENAME + "\nCheck if the file" +CONFIGFILENAME+ " is well formed")
            config_file_raw.close()
            raise
        finally:
            config_file_raw.close()
        config_file_raw.close()
        logging.info("Config File "+CONFIGFILENAME+" valid.")
                
        #setting the bucket name
        
        #Setting AWS credentials
        logging.info("Checking if number of files on S3 bucket > 0")
        global S3   
        S3 = boto3.resource('s3',
            aws_access_key_id= config_file['AWSAccess'],
            aws_secret_access_key=config_file['AWSSecret'],
            config=Config(signature_version='s3v4')
            )
        global CLIENT
        CLIENT = boto3.client('s3',
            aws_access_key_id=config_file['AWSAccess'],
            aws_secret_access_key=config_file['AWSSecret'],
            config=Config(signature_version='s3v4')
        )
        
        #seting the bucket name
        GLOBALPARAMS['BUCKET_NAME'] = "Team" + str(GLOBALPARAMS['TEAMNO']) + GLOBALPARAMS['STATE'] +"Assignment1"
        global BUCKET_NAME
        BUCKET_NAME = GLOBALPARAMS['BUCKET_NAME']
                    
        #creating bucket if it doesn't exist
        try:
            S3.create_bucket(Bucket=GLOBALPARAMS['BUCKET_NAME'])
        except:
            logging.error("Error while creating / accessing new Bucket")
            raise
        logging.info("Bucket created / already exists")

        #Checking if files exist in the bucket
        objList = CLIENT.list_objects(Bucket=str(GLOBALPARAMS['BUCKET_NAME']),Delimiter='/')
        if not objList.get('Contents'):
            logging.warn("Bucket empty. Will use configFileIntial.json")
            GLOBALPARAMS['USEINITALCONFIG'] = True

        if GLOBALPARAMS['USEINITALCONFIG'] == True:
            try:
                logging.info("Since Bucket empty, Attempting to read configuration from "+INITIALCONFIGFILENAME)
                init_config_file_raw = open(INITIALCONFIGFILENAME)
                init_config_file = json.load(init_config_file_raw)
                if not validateConfigFile(init_config_file):
                    #self._dataprepstat = False
                    raise
                #Set parameters derived from file
                GLOBALPARAMS['NOAALINKS'] = init_config_file['link']

            except:
                logging.error("Error opening /parsing the initial config file "+ INITIALCONFIGFILENAME +"\nCheck if the file "+INITIALCONFIGFILENAME+" is well formed and if all keys (state,team,AWSAccess,AWSSecret,notificationEmail,weatherStation) exist")
                #self._dataprepstat = False
                #sys.exit(1)
                init_config_file_raw.close()
                raise
            finally:
                init_config_file_raw.close()
            
            init_config_file_raw.close()
            logging.info("Initial Config File "+INITIALCONFIGFILENAME+" valid.")
                
            

        with self.output().open("w") as fp:
            json.dump(GLOBALPARAMS, fp)
        
        logging.info("Data Ingestion Preprocessing completed."+ INITIALCONFIGFILENAME + " written. This will be deleted after the job is complete.")
        
    def output(self):
        return luigi.LocalTarget(SAVEDCONFIGFILENAME)

class CustomDataIngestionException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)