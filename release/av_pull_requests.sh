

# echo "####### Build av-sched UI #######"
# npm install -g gulp
# npm install -g bower
# bower install
# gulp build


echo "####### Build av-sched & Run unit tests #######"
mvn -e -q clean package


echo "####### TODO - Run IT #######"