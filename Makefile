.PHONY: all
all: format build

.PHONY: format
format:
	sh mvn spring-javaformat:apply

.PHONY: build
build: ## build java applications
	sh mvn clean install

.PHONY: create-topics
create-topics:
	docker exec -it kafka kafka-topics --create --topic tp_meetup_input --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181/; \
	docker exec -it kafka kafka-topics --create --topic tp_meetup_agg --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181/; \
	docker exec -it kafka kafka-topics --create --topic tp_meetup_output --partitions 2 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181

.PHONY: input-output
input-output: ## starts ksql datagen and jdbc sink
	cd datagen-source-connector/; \
	sh deploy.sh
	cd jdbc-sink-connector/; \
	sh deploy.sh

.PHONY: start infrastructure
up-infra: ## start enviroment
	set COMPOSE_CONVERT_WINDOWS_PATHS=1/; \
	docker-compose -f docker-compose.yml up -d --build

.PHONY: start product
up-product: # start kafka streams ms
	docker-compose -f docker-compose-product.yml up -d --build

.PHONY: stop all
down: ## stop enviroment
	docker-compose down

.PHONY: destroy
destroy: ## cleanup environment
	docker-compose down --remove-orphans

.PHONY: wait
wait: ## wait for init of infra
	sleep 8

.PHONY: start
start: up-infra wait create-topics ## wait for init of infra

