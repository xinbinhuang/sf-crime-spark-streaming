.PHONY: init cluster simulate kafka_consume spark_streaming

init:
	pipenv install 

cluster:
	docker-compose up -d

simulate:
	pipenv run python kafka_server.py

kafka_consume:
	pipenv run python consumer_server.py

spark_streaming:
	pipenv run spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 spark_streaming.py
