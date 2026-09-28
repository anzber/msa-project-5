# Задание 6: API запуска задачи и трейсинг в логах

## 1. Что добавлено к решению задачи 5

* ETL-задача больше не запускается при старте приложения
  (`spring.batch.job.enabled=false`): её запускает `POST
  /api/jobs/import-products` (`JobController`).
* Появился простой клиент `client/client.py`, который вызывает этот endpoint.
* В каждую строку лога добавлены **traceId**, **spanId** и **URI** запроса.
* Логи по-прежнему уходят по цепочке filebeat -> Logstash -> Elasticsearch и
  смотрятся в Kibana.

## 2. Выбор решения для трейсинга

| Вариант | Плюсы | Минусы | Решение |
|---|---|---|---|
| **Micrometer Tracing + Brave** | Нативно для Spring Boot 3, уже подключён в проекте (`micrometer-tracing-bridge-brave`), traceId/spanId автоматически попадают в MDC, поддержка W3C Trace Context | Нужно отдельно добавить URI в MDC | **Выбрано** |
| Spring Cloud Sleuth | Привычный API | Для Spring Boot 3 не поддерживается (проект закрыт), заменяется на Micrometer Tracing | Отклонено |
| OpenTelemetry Java agent | Авто-инструментирование без правок кода | Тяжёлый javaagent, сложнее конфигурировать в контейнере, избыточен для одной задачи логирования | Отклонено |
| Ручной MDC без библиотеки | Нет зависимостей | Нужно самому генерировать и протаскивать идентификаторы, нет стандарта и propagations | Отклонено |

Итог: **Micrometer Tracing с Brave** как bridge и **W3C Trace Context** как
формат propagation.

## 3. Как трейсинг попадает в логи

1. Spring Boot 3 автоматически инструментирует входящие HTTP-запросы. На каждый
   запрос создаётся спан, а `traceId` и `spanId` кладутся в SLF4J MDC.
2. `logback-spring.xml` уже настроен на JSON-логи через
   `logstash-logback-encoder`: провайдеры `<pattern>` выводят `trace` и `span`, а
   провайдер `<mdc/>` выводит все остальные ключи MDC.
3. URI запроса в MDC не попадает: он оказывается только в атрибутах спана.
   Поэтому добавлен `RequestUriMdcFilter`, который кладёт в MDC ключ `uri` со
   значением `<МЕТОД> <URI>` на время обработки запроса.
4. Запуск job синхронный (`JobLauncher` по умолчанию выполняет job в потоке
   запроса), поэтому логи шагов ETL наследуют тот же MDC: в одной строке видно
   `traceId`, `spanId` и `uri` вместе.

Формат строки лога (фрагмент):

```json
{
  "trace": "4f9c...e1",
  "span": "a1b2c3d4e5f60718",
  "uri": "POST /api/jobs/import-products",
  "app": "batch-processing",
  "logger": "com.example.batchprocessing.JobController",
  "level": "INFO",
  "msg": "API request received: source=python-client"
}
```

## 4. Propagation между клиентом и сервером

Клиент `client.py` генерирует `traceId`/`spanId` и отправляет стандартный
заголовок `traceparent` (W3C Trace Context):

```text
traceparent: 00-<32 hex traceId>-<16 hex spanId>-01
```

Сервер продолжает этот трейс, поэтому `traceId`, напечатанный клиентом,
совпадает с `traceId` во всех логах обработки запроса, включая шаги job. Это
позволяет в Kibana найти весь путь одного вызова по одному идентификатору.

Для демонстрации сэмплирование включено полностью
(`management.tracing.sampling.probability=1.0`), иначе часть запросов не
трассировалась бы.

## 5. Запуск и проверка

```bash
# поднять стек
docker compose up -d --build

# вызвать API из клиента
python3 ../client/client.py --url http://<host>:8082 --source python-client
```

В ответе API возвращаются `traceId`, `spanId` и `uri`. Тот же `traceId` можно
найти в Kibana (Discover, индекс `filebeat-*`) по строке поиска
`traceId: "<значение>"` и увидеть все логи запроса и запущенного job.

## 6. Как посмотреть в Kibana

* Discover, index-pattern `filebeat-*`;
* колонки: `traceId`, `spanId`, `uri`, `logger`, `msg`;
* фильтр, например `uri: "POST /api/jobs/import-products"` или `traceId: "<id>"`.
