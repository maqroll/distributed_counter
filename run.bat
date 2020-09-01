docker-compose -f ensemble.yml down
docker-compose -f ensemble.yml up --scale client-1=0 --scale client-2=0