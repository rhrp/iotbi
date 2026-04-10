#!/bin/bash
#
# Author: rui Humberto Pereira <rui.humberto.pereira@gmail.com>
# Date: 2025/Ago/27
# 
ENV="NODE_ENV=production TZ=Europe/Lisbon DEBUG=iotbi.* IOTBI_PORT=5000 MCP_APPKEY=rhp IOTBI_USE_HTTPS=false "
COMMAND="node --max-old-space-size=8192 ./bin/boot.js"
FILE_LOG="/tmp/iotbi/iotbi.log"
FILE_PID="/tmp/iotbi/iotbi.pid"
PATH_CACHE="/tmp/iotbi/cache"

#
# Functions
#
function getPid()
{
  if [ -f "${FILE_PID}" ];
  then
     let PID=`cat ${FILE_PID}`
     echo ${PID}
  else
     echo -1
  fi
}
function isRunning()
{
   # Set space as the delimiter
   IFS=' '
   # Read the split words into an array
   # based on space delimiter
   read -ra newarr <<< "$COMMAND"

   GREP_OPTIONS=" "
   # Print each value of the array by using
   # the loop
   for val in "${newarr[@]}";
   do
      GREP_OPTIONS="${GREP_OPTIONS} -e $val"
   done

   PID=`getPid`
   if [ ${PID} -gt 1 ];
   then
       COUNT=`ps aux|grep ${PID}|grep ${GREP_OPTIONS}|wc -l`
       if [ "${COUNT}" == "1" ];
       then
          echo ${PID}
       else
          echo -2
       fi
   elif [ ${PID} -eq -1 ];
   then
      echo -1
   else
      echo 0
   fi
}
function start()
{
  if [ -f "${FILE_PID}" ];
  then
     PID=`getPid`
     echo "The process is already running (pid=${PID})!"
     echo "Fail starting service. There is a running process using PID=${PID}">>"${FILE_LOG}"
     return;
  fi
  echo "Service started">>"${FILE_LOG}"
  sleep 1
  if [ "${ENV}" != "" ];
  then
    export ${ENV} 
  fi
  nohup ${COMMAND}>>${FILE_LOG} 2>&1 &
  echo $! > "${FILE_PID}"
}
function stop()
{
   STATUS=`isRunning`
   if [ ${STATUS} -gt 1 ];
   then
      echo "Kill process pid=${STATUS}!"
      kill -9 ${STATUS}
      rm ${FILE_PID}
      echo "Service stopped">>"${FILE_LOG}"

   elif [ ${STATUS} -eq -1 ];
   then
      echo "The service is not running!"
   elif [ ${STATUS} -eq -2 ];
   then
      echo "Service is not working due to an abrupt shutdown!"
   else
      echo "The service PID (${STATUS}) is not valid"
   fi
}
function status()
{
   STATUS=`isRunning`
   if [ ${STATUS} -gt 1 ];
   then
      echo "The service is running (pid=${STATUS}) ;-)"
   elif [ ${STATUS} -eq -1 ];
   then
      echo "The service is not running!"
   elif [ ${STATUS} -eq -2 ];
   then
      echo "Service is not working due to an abrupt shutdown!"
   else
      echo "The service PID (${STATUS}) is not valid. Remove manualy ${FILE_PID}"
   fi
}
function clearCache()
{
   echo "Cleaning the cache"
   #The last pipe (tr) replaces \n by a space to enable the split.  IFS='\n' does not wored :-(
   CACHE_CONTENT=`ls ${PATH_CACHE}|grep -e "[a-z0-9]\{64\}.parquet" -e "[a-z0-9]\{64\}.csv" -e "[a-z0-9]\{64\}.json"|tr "\n" " "`
   IFS=' '
   # Read the split words into an array
   # based on space delimiter
   read -ra newarr <<< "${CACHE_CONTENT}"

   for cachefile in "${newarr[@]}";
   do
     echo "Removing ${cachefile}"
     rm "${PATH_CACHE}/${cachefile}"
   done
   echo "File(s) removed: ${#newarr[@]}"
}
function showLastLogs()
{
  if [ -f "${FILE_LOG}" ];
  then
     tail -n 200 ${FILE_LOG}
     return;
  fi
  echo "There is not available Logs"
}

#
# Main
#
case "$1" in
    "start")
        start  
        ;;
    "stop")
        stop
        ;;
    "status")
        status
        ;;
    "logs")
        showLastLogs
        ;;
    "clearcache")
        clearCache
        ;;
    *)
        echo "Usage: $0 start|stop|status|logs|clearcache"
esac
