luigi --module dataIngestion UploadRawDataToS3 --local-scheduler
echo "Luigi Tasks completed"
rm -f $OUTPUTPATH/TEMP*
