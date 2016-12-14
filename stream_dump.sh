DATA=`cat /tmp/csvdump`
for DATUM in $DATA
do
    echo "Sending `date  +'%Y%m%d-%H%M%S%N'`"
    echo $DATUM >> /tmp/datastream/`date  +'%Y%m%d-%H%M%S%N'`.txt
    #sleep 1
done

