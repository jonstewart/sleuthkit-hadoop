#!/bin/sh

# /*
#    Copyright 2011, Lightbox Technologies, Inc
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# */


# tpdemo is similar to tpkickoff, but optimized for running on a standalone VM where HDFS is not running


#set -x

if [ $# -ne 3 ] && [ $# -ne 4 ]
then
  echo "Usage: tpdemo.sh image_friendly_name image_path jar_dir"
  exit 1
fi

if [ $# -eq 4 ]
then
  if [ ${#4} -ne 32 ]
  then
    echo "Invalid Image ID: Must be 32 digits long"
    exit 1
  fi
fi

pwd
date

FriendlyName=$1
ImagePath=$2
JarDir=$3

#export LD_LIBRARY_PATH=/home/uckelman/projects/lightbox/fsrip/deps/lib
FSRIP=/home/uckelman/projects/lightbox/fsrip/build/src/fsrip

HADOOP=/usr/bin/hadoop

JarFile=`ls -r $JarDir/sleuthkit-pipeline-r*-job.jar | head -n 1`
if [ $? -ne 0 ]; then
  echo "failed to find pipeline JAR"
  exit 1
fi

JsonFile=$FriendlyName.json

echo "jar file is ${JarFile}"

# rip filesystem metadata, upload to hdfs
$FSRIP dumpfs $ImagePath > $JsonFile
if [ $? -ne 0 ]; then
  echo "image metadata extract failed"
  exit 1
fi
echo "done extracting metadata"

# upload image to hdfs
ImageID=`$FSRIP dumpimg $ImagePath | md5sum -q`
if [ $? -ne 0 ]; then
  echo "could not calculate hash value"
  exit 1
fi
echo "done calculating hash"

if [ $# -eq 4 ]; then
  ImageID=$4
fi
echo "Image ID is ${ImageID}"

# copy reports template
$HADOOP fs -cp /texaspete/templates/reports /texaspete/data/$ImageID/
if [ $? -ne 0 ]; then
  echo "copying reports template failed"
  exit 1
fi
echo "reports template copied"

# create directories
DataDir=/texaspete/data/${ImageID}/reports/data
mkdir -p $DataDir

# put MD5 and friendly name into js file
echo "var deviceInfo = [{\"a\":\"deviceName\",\"n\":\"${FriendlyName}\"},{\"a\":\"md5\",\"n\":\"${ImageID}\"}]" > ${DataDir}/basic.js

# rip image info, insert in hbase
InfoJs=${DataDir}/info.js
echo "var fsInfo =" > InfoJs
$FSRIP info $ImagePath >> InfoJs
cat $InfoJs | $HADOOP jar $JarFile com.lightboxtechnologies.spectrum.InfoPutter $ImageID $FriendlyName
if [ $? -ne 0 ]; then
  echo "image info registration failed"
  exit 1
fi
echo "image info registered"

# kick off ingest
$HADOOP jar $JarFile org.sleuthkit.hadoop.pipeline.Ingest $ImageID $ImagePath $JsonFile $FriendlyName
if [ $? -ne 0 ]; then
  echo "ingest failed"
  exit 1
fi
echo "done with ingest"

# kick off pipeline
$HADOOP jar $JarFile org.sleuthkit.hadoop.pipeline.Pipeline $ImageID $FriendlyName
if [ $? -ne 0 ]; then
  echo "pipeline failed"
  exit 1
fi
echo "pipeline completed"

date
