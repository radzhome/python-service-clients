# Python Service Clients

Service clients for common aws, cache and search services

### Testing using local services

1. With docker installed, bring up docker services `docker-compose up -d`
1. Run tests `cd tests && python3 test_redis_client.py`
1. Run tests `cd tests && python3 test_es_client.py`
1. Run tests `cd tests && python3 test_rabbit_client.py`
1. Run tests `cd tests && python3 test_redis_queue_client.py`


### Testing aws modules

1. Update keys in test_s3_client.py
1. Run tests `cd tests && python3 test_s3_client.py`
1. Update keys in test_sqs_client.py
1. Run tests `cd tests && python3 test_sqs_client.py`
