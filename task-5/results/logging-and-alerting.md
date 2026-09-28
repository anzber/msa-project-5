# Обоснование сбора логов и настройки оповещений

## 1. Сбор логов

### 1.1 Выбранный способ

Структурированные JSON-логи приложения выводятся в stdout контейнера, откуда их
забирает **filebeat** (Docker input) и отправляет в **Logstash**, а тот
индексирует их в **Elasticsearch**. Просмотр выполняется в **Kibana**.

```text
Spring Batch app (logback + logstash-logback-encoder, JSON)
  -> stdout контейнера (docker json-file)
     -> filebeat (читает /var/lib/docker/containers)
        -> Logstash (парсинг, index => filebeat-<version>-YYYY.MM.dd)
           -> Elasticsearch
              -> Kibana (Discover, index-pattern filebeat-*)
```

Ключевые настройки:

* приложение пишет JSON через `logback-spring.xml` и
  `net.logstash.logback:logstash-logback-encoder`: поля `ts`, `level`, `logger`,
  `app`, `msg`, `trace`, `span`, `thread`, `stack`;
* файл `filebeat/filebeat.yml`: input `type: docker`, чтение
  `/usr/share/dockerlogs/data`, процессоры `decode_json_fields` (разворачивает
  JSON в поля), `add_docker_metadata` (метаданные контейнера) и `drop_event`,
  который оставляет только логи контейнера приложения;
* `logstash/pipeline/logstash.conf`: input `beats` (порт 5044), фильтр `date` по
  полю `ts`, output в Elasticsearch с индексом `filebeat-<version>-YYYY.MM.dd`.

### 1.2 Почему так, а не иначе

| Вариант | Плюсы | Минусы | Решение |
|---|---|---|---|
| **filebeat** + Logstash + ES | Легкий агент, официальный из семейства Elastic Beats, читает docker-логи без изменений в приложении, есть буферизация и повторная отправка | Нужен доступ к docker-логам (монтирование) | **Выбрано** |
| Logstash как агент на каждом узле | Один инструмент для сбора и обработки | Тяжелый (JVM) на каждом узле, избыточен для сбора логов | Отклонено |
| Прямой logback-appender в Elasticsearch | Нет отдельного агента | Приложение связано с адресом ES, нет буфера при недоступности ES, логи теряются при сбое | Отклонено |
| Fluentd / Fluent Bit | Хорошая альтернатива filebeat | Меньше готовых примеров под docker json-file в этом стеке; выбранный вариант ближе к шаблону задания | Отклонено |

Вывод: filebeat дает минимальное вмешательство в приложение (логи как обычно
идут в stdout), устойчив к недоступности Logstash/ES и хорошо масштабируется по
узлам в облаке.

### 1.3 Почему JSON-логи

Формат JSON позволяет искать и фильтровать по полям (`level`, `logger`, `app`,
`trace`, `span`), а не по подстрокам в тексте. Поля `trace`/`span` закладывают
основу для трейсинга (задание 6): по `traceId` можно собрать все записи одного
запуска.

## 2. Оповещения

### 2.1 Выбранный способ

Оповещения настраиваются **правилами Prometheus** (`prometheus/alert.rules.yml`),
состояние алертов видно в интерфейсе Prometheus (раздел Alerts). Grafana
используется для визуализации метрик дашбордом.

Правила:

| Алерт | Выражение | for | Severity | Смысл |
|---|---|---|---|---|
| `BatchJobFailed` | `batch_job_last_success{job="batch-processing"} == 0` | 10s | critical | Последний запуск `importProductJob` завершился не со статусом COMPLETED |
| `BatchAppDown` | `up{job="batch-processing"} == 0` | 20s | critical | Prometheus не может опросить приложение |

### 2.2 Prometheus или Grafana: обоснование

| Критерий | Prometheus rules | Grafana alerting |
|---|---|---|
| Источник истины | Оценка правил там же, где хранятся метрики | Оценка поверх Prometheus, но правила живут в Grafana |
| Версионирование | Файл правил рядом с конфигом Prometheus, в git | Правила в Grafana (или provisioning-файлы), отдельный слой |
| Работа без UI | Правила и состояние `firing` не зависят от Grafana | Для оценки алертов нужна Grafana |
| Маршрутизация уведомлений | Через Alertmanager (стандартный путь) | Встроенные contact points |
| Визуализация | Минимальная (UI Prometheus) | Богатые дашборды и единый UI алертов |

Решение: **оценка и состояние алертов - в Prometheus** (это ближе к данным и не
зависит от Grafana), а **Grafana - для дашбордов**. В продакшене к Prometheus
добавляется **Alertmanager** для маршрутизации уведомлений (email/Slack); в
локальном POC его роль не обязательна, поэтому показывается состояние `firing` в
Prometheus.

### 2.3 Как демонстрируется срабатывание

* `BatchJobFailed`: приложение запускается с переменной окружения
  `BATCH_SIMULATE_FAILURE=true`, из-за чего обработка одной строки намеренно
  завершается ошибкой, job получает статус FAILED, gauge
  `batch_job_last_success` становится 0 и правило переходит в `firing`.
* `BatchAppDown`: останавливается контейнер приложения (`docker compose stop
  app`), метрика `up` становится 0 и правило переходит в `firing`.

Оба сценария воспроизводимы и показаны на скриншотах.

## 3. Сводка по стеку

```text
Метрики:  Micrometer -> /actuator/prometheus -> Prometheus -> (Grafana + alert rules)
Логи:     JSON (logback) -> stdout -> filebeat -> Logstash -> Elasticsearch -> Kibana
Алерты:   Prometheus rules (BatchJobFailed, BatchAppDown); в prod + Alertmanager
```
