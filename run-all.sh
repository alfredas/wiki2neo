#!/bin/sh
array=(one two three four [5]=five)
for item in ${array[*]}
do
    sh run.sh $item
done
