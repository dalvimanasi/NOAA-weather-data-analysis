export CRONPATH=/home/vishalsatam/cronAssign1
filename=$CRONPATH/logs/$(date "+%d%m%Y%H%m%s").log
echo $filename
touch $filename
echo  Starting Cron Job >>$filename
echo  Creating Ingestion Job >>$filename
docker create --name="dataingest" vishalsatam1988/assign1dataingestion
echo  Copying todays config.json and configInitial.json to datawrangle >>$filename
docker cp $CRONPATH/dataingestionconfig/config.json dataingest:/src/assignment1/
docker cp $CRONPATH/dataingestionconfig/configInitial.json dataingest:/src/assignment1/
echo  Starting dataingest container >>$filename
docker start -i dataingest
echo  Commiting todays updates to vishalsatam1988/assign1dataingestion >>$filename
docker commit dataingest vishalsatam1988/assign1dataingestion
echo  Removing completed dataingest container >>$filename
docker rm dataingest
echo  Ingestion Job Completed >>$filename
echo  Creating Wrangling Job >>$filename
docker create --name="datawrangle" vishalsatam1988/assign1datawrangling
echo  Copying todays configwrangle.json to datawrangle >>$filename
docker cp $CRONPATH/datawrangleconfig/configWrangle.json datawrangle:/src/assignment1/
echo  Starting datawrangle container >>$filename
docker start -i datawrangle
echo  Commiting todays updates to vishalsatam1988/assign1datawrangling>>$filename
docker commit datawrangle vishalsatam1988/assign1datawrangling
echo  Removing completed datawrangle container >>$filename
docker rm datawrangle
echo  Wrangling Job Completed >>$filename
echo Cron Job Completed >>$filename
