## ----------------------------------------------------------------------
## Команды для запуска и остановки кластера kafka
## ----------------------------------------------------------------------

help: ## Выводит эту подсказку
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

run_cluster:
	docker-compose -f docker/docker-compose.yml --profile cluster up -d

create_topic:
	sleep 5s
	docker exec kafka-1 sh -c "/bin/kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --topic lesson-2 --partitions 3 --replication-factor 2"
	docker exec kafka-1 sh -c "/bin/kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe"

run: run_cluster create_topic run_producers run_single_message_consumer run_batch_consumer ## Запускает кластер, создает топик, запускает producer, batch-consumer, single-message-consumer приложения

cluster_stop: ## Останавливает кластер
	docker-compose -f docker/docker-compose.yml down

run_producers:
	docker-compose -f docker/docker-compose.yml --profile producer up -d

run_single_message_consumer:
	docker-compose -f docker/docker-compose.yml --profile single-message-consumer up -d

run_batch_consumer:
	docker-compose -f docker/docker-compose.yml --profile batch-consumer up -d