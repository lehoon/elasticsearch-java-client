@echo off

mvn deploy:deploy-file -DgroupId=com.finesys -DartifactId=elasticsearch-playback -Dversion=%1 -Dpackaging=jar -Dfile=target/elasticsearch-playback-%1.jar -Durl=http://maven.lehoon.com/content/repositories/thirdparty/ -DrepositoryId=lehoon-nexus

