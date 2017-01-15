#!/bin/bash
jobName=htable

#Get current folder
curent_dir=$(pwd)

nameJar=HBaseTable.jar
jarPath=$curent_dir/Jars/$nameJar
classPath=com.hbase.htable.HBaseTable

echo "Step 1: Create Jar filename"

if [ ! -f $jarPath ]
then
	echo "Create new jar driver file"
	cd $curent_dir/HTable/target/classes
	jar -cvfm $nameJar $jobName.MF *
	mv -f $nameJar $curent_dir/Jars
	cd $curent_dir
fi

echo "Step 2: Run project"
java -jar $curent_dir/Jars/$nameJar

read -p "Completed!"

