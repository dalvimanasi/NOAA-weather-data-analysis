luigi --module wrangle UploadCleanDataToS3 --local-scheduler
echo "Wrangling Tasks completed"
rm -f $OUTPUTPATH/TEMP*
echo "Please check "+ $LOGPATH + " for detailed logs"
echo "Please commit the running docker container to avoid losing changes"
echo "Execute the following command for starting jupyter notebook. Password = keras  :::: /bin/bash -c 'jupyter notebook --no-browser --allow-root --ip=* --NotebookApp.password=\"\$PASSWD\" \"\$@\"'"
