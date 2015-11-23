#!/bin/sh

SERVER="http://download.wikimedia.org/enwiki/20110901/"
EXT=".7z"
FILENAME=$1

if [ -e ${FILENAME}${EXT} ]
then
  echo "Found $FILENAME"
else
  echo "Will download $FILENAME"
  wget ${SERVER}${FILENAME}${EXT}
fi

p7zip -d ${FILENAME}${EXT}
java -jar wiki2neo-0.0.1-SNAPSHOT.jar ${FILENAME}


