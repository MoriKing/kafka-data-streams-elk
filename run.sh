set -e
gradle clean test

gradle kafka-producer:jibdockerbuild
gradle kafka-streams:jibdockerbuild
gradle kafka-consumer:jibdockerbuild

docker-compose -f ./kafka.yaml up