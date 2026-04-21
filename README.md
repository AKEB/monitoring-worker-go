# monitoring-worker-go

Go-реализация воркера мониторинга, совместимая по API с PHP-версией (`monitoring-worker`): один бинарник, конфиг из окружения и опционально из `.env` рядом с процессом или с бинарником.

## Сборка

```bash
go build -buildvcs=false -o monitoring-worker ./cmd/worker/
```

Если репозиторий без полноценного git и сборка ругается на VCS, флаг `-buildvcs=false` обязателен.

## Запуск

1. Скопируйте `.env.example` из PHP-воркера или задайте те же переменные (минимум `SERVER_HOST`, `WORKER_KEY_HASH`).
2. Положите `.env` в текущую директорию или рядом с исполняемым файлом.
3. `./monitoring-worker`

После команды `restart` от сервера процесс завершает цикл и печатает `Exiting` (как в PHP после `loop()`).

### Поведение при частых ошибках (как в PHP)

При `finished with error` время следующего запуска выставляется как `update_time = now - repeat_seconds`, поэтому задача снова становится доступной почти сразу. При малом `LOOP_TIMEOUT` в логах с `DEBUG=true` будет много строк `Start`/`Finish` — это ожидаемо; отключите `DEBUG` или увеличьте `LOOP_TIMEOUT`, если шум мешает.

Очередь отправки на сервер **дедуплицируется по `job_id`**: в одном интервале `RESPONSE_SEND_TIMEOUT` уходит один актуальный результат на задачу (без сотен повторов в одном POST).

Чтобы после ошибки/таймаута **не запускать проверку снова на каждом тике цикла** (как в PHP при `update_time = now - repeat_seconds`), выставьте `IMMEDIATE_ERROR_RETRY=false`: тогда `update_time = now` и следующий запуск будет не раньше чем через `repeat_seconds`.

## Переменные окружения

Смысл и имена совпадают с `monitoring-worker`: `TZ`, `SERVER_HOST`, `WORKER_KEY_HASH`, `WORKER_THREADS`, `JOBS_GET_TIMEOUT`, `LOOP_TIMEOUT`, `RESPONSE_SEND_TIMEOUT`, `LOGS_WRITE_TIMEOUT`, `PROXY_HOST`, `PROXY_TYPE`, `WORKER_VERSION`, `DEBUG`, `CURL_DEBUG`, `DOCKER_DEBUG`, `IMMEDIATE_ERROR_RETRY` (по умолчанию `true`, как в PHP).

### `WORKER_VERSION` (как в PHP)

В PHP в теле запросов к `/api/monitoring/get/` и `/api/monitoring/state/` уходит поле `worker_version` из **`getenv('WORKER_VERSION')`** (`Config.php`). В Docker-образе PHP то же значение прокидывается через `ARG/ENV WORKER_VERSION` и при сборке переписывается `version.php`, но для рантайма решает именно переменная окружения.

В Go:

1. если задан **`WORKER_VERSION`** в окружении или в `.env` — в API уходит он;
2. иначе — строка из **`-ldflags "-X monitoring-worker-go/internal/buildinfo.Version=..."`** (в GitHub Actions подставляется имя ref: тег `v1.2.3` или `dev-<short sha>`);
3. иначе — **`local`**, как значение по умолчанию в `monitoring-worker/src/version.php`.

Локальная сборка без ldflags и без env: в запросах будет `worker_version: "local"`. Чтобы совпасть с релизом, собирайте с `-ldflags` или задайте `WORKER_VERSION` в `.env`.

### Отладка Docker Engine API

В `.env` включите **`DOCKER_DEBUG=true`** (отдельно от `DEBUG`): в stderr пойдут строки с префиксом `[docker]` — URL запроса, TLS/mTLS (наличие PEM, ошибки `LoadX509KeyPair`), прокси, код ответа, превью тела. Это повторяет идею `DOCKER_DEBUG` в PHP `docker.php`.

Если в задаче **`host` — IP**, а сертификат демона выдан на **DNS** (например `*.example.com`), в JSON задачи можно передать **`tls_server_name`** — оно попадёт в SNI и в проверку имени, при этом URL к Engine API по-прежнему строится из `host` и `port`. Ошибка вида `x509: certificate signed by unknown authority` при уже загруженном `tls_ca_file` обычно означает неверную или неполную цепочку в PEM (нужен CA/промежуточные, которыми реально подписан **серверный** сертификат демона, а не только клиентский mTLS).

## CI и релизы

Файл `.github/workflows/build.yml`:

- на **push** в `main`/`master` и на **pull request** — сборка и артефакт в карточке запуска workflow (как раньше);
- на **push тега** вида `v1.2.3` — та же сборка плюс **GitHub Release** с вложением `monitoring-worker` (через [softprops/action-gh-release](https://github.com/softprops/action-gh-release)).

Пример публикации версии:

```bash
git tag v1.0.0
git push origin v1.0.0
```

Обычный push в ветку **релиз не создаёт** — только артефакт в Actions.
