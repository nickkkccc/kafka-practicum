## ----------------------------------------------------------------------
## Команды для запуска и остановки кластера kafka
## ----------------------------------------------------------------------

help: ## Выводит эту подсказку
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST)

run_cluster: ## Запускает кластер описанный через docker-compose.yml в директории docker
	docker-compose -f docker/docker-compose.yml --profile cluster up -d

create_topic: ## Создает топик с названием lesson-2
	sleep 5s
	docker exec kafka-1 sh -c "/bin/kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --topic lesson-2 --partitions 3 --replication-factor 2"
	docker exec kafka-1 sh -c "/bin/kafka-topics --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --describe"

run: run_cluster create_topic run_producers ## Запускает кластер и создает топик

cluster_stop: ## Останавливает кластер
	docker-compose -f docker/docker-compose.yml down

run_producers: ## Запускает приложения Producer
	docker-compose -f docker/docker-compose.yml --profile producer up -d
