# Python Service Clients

Service clients for common aws, cache and search services

### Testing redis, es using local services

1. With docker installed, bring up redis and es `docker-compose up -d`
1. Run tests `cd tests && python3 test_redis_client.py`
1. Run tests `cd tests && python3 test_es_client.py`


# Testing aws modules

1. Update keys in test_s3_client.py
1. Run tests `cd tests && python3 test_s3_client.py`
1. Update keys in test_sqs_client.py
1. Run tests `cd tests && python3 test_sqs_client.py`
