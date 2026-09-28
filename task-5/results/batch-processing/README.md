# Task 5: мониторинг, логирование и оповещение (Spring Batch)

Стек: Spring Batch ETL-приложение + Prometheus + Grafana + ELK (filebeat,
Logstash, Elasticsearch, Kibana). Все сервисы поднимаются одним
`docker compose up`.

## Состав

| Сервис | Что делает | Порт на хосте |
|---|---|---|
| app | Spring Batch ETL, `/actuator/prometheus`, JSON-логи | 8081 -> 8080 |
| postgresdb | PostgreSQL: products, loyality_data, метаданные Batch | 5434 -> 5432 |
| prometheus | Скрейп метрик, правила алертов | 9090 |
| grafana | Дашборд по метрикам | 3000 |
| filebeat | Сбор docker-логов приложения | - |
| logstash | Разбор логов, индексация | 5044 |
| elasticsearch | Хранилище логов | 9210 -> 9200 |
| kibana | Просмотр логов | 5601 |

> Порты и имена контейнеров отличаются от шаблона, потому что на хосте уже
> заняты 8080, 9200/9300, 5432 и имя контейнера `elasticsearch`. Проект Compose
> называется `msa-task5`, контейнеры - `task5-*`.

## Сборка и запуск

```bash
# сборка jar
./gradlew build -x test
# либо в контейнере, если нет локального JDK:
docker run --rm --user $(id -u):$(id -g) -e GRADLE_USER_HOME=/tmp/gradle \
  -v "$PWD":/project -w /project gradle:8.8-jdk17 gradle build --no-daemon -x test

# поднять весь стек (таблицы создаются автоматически из schema-all.sql)
docker compose up -d --build
```

Grafana: `http://<host>:3000` (admin/admin), дашборд `Batch processing - monitoring`.
Prometheus: `http://<host>:9090` (Targets, Alerts).
Kibana: `http://<host>:5601`, index-pattern `filebeat-*`.

## Демонстрация оповещений

```bash
# срабатывание BatchJobFailed: job падает намеренно
BATCH_SIMULATE_FAILURE=true docker compose up -d --force-recreate app

# срабатывание BatchAppDown: приложение недоступно
docker compose stop app

# вернуть здоровое состояние
BATCH_SIMULATE_FAILURE=false docker compose up -d --force-recreate app
```

## Полезные команды

```bash
# метрики приложения
curl -s localhost:8081/actuator/prometheus | grep '^batch_'

# состояние алертов Prometheus
curl -s localhost:9090/api/v1/alerts

# индексы Elasticsearch
curl -s localhost:9210/_cat/indices?v

# логи контейнеров
docker compose logs -f app
```
