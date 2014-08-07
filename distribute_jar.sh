echo `pwd`
for i in {40..43};
do scp -r properties/default.properties compg${i}:`pwd`/properties/;scp ${1}.jar compg${i}:`pwd`/;
done;
