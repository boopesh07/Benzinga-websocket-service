## Benzinga Newsfeed WebSocket sink (to S3 or local files)

Streams Benzinga Newsfeed WebSocket messages and writes minimal NDJSON records to S3 or local files. Only messages that include at least one security with a `symbol` are written.

- Extracted fields: `timestamp`, `ticker`, `news_id`
- Heartbeat, auto-reconnect with backoff
- Hourly windowed rotation with size limits; S3 multipart uploads and marker files

Docs: [Newsfeed WS](https://docs.benzinga.com/benzinga-apis/newsfeed-stream-v1/get-stream)

### Configuration

Provide environment variables (or an `.env` file):

- `BENZINGA_API_KEY` (required)
- `BENZINGA_WS_URL` default `wss://api.benzinga.com/api/v1/news/stream`
- `SINK` `file`|`s3` (default `file`)
- File sink: `FILE_DIR` default `./data`
- S3 sink: `S3_BUCKET` (required), `S3_PREFIX` default `benzinga/news`, `AWS_REGION` optional
- WebSocket: `PING_INTERVAL` default `30`, `PING_TIMEOUT` default `10`
- Reconnect: `RECONNECT_BASE_DELAY` default `1.0`, `RECONNECT_MAX_DELAY` default `60.0`
- Logging: `LOG_LEVEL` default `INFO`, `LOG_FORMAT` `text`|`json` (default `text`)

Authentication is done by appending the API token as the `token` query parameter to the WebSocket URL.

### Run locally (file sink)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export SINK=file
export FILE_DIR=./data
export BENZINGA_API_KEY=***
python -m app.main
```

Files will appear under `./data/ingest_dt=YYYY/MM/DD/hour=HH/part=.../*.ndjson`.

### Switch to S3

```bash
export SINK=s3
export S3_BUCKET=your-bucket
export S3_PREFIX=benzinga/news
export AWS_REGION=us-east-1
python -m app.main
```

### Docker

```bash
docker build -t bz-ws-sink .
# file sink
docker run --rm -e SINK=file -e FILE_DIR=/data -e BENZINGA_API_KEY=*** -v $(pwd)/data:/data bz-ws-sink
# s3 sink (uses .env for secrets)
docker run --rm --env-file .env bz-ws-sink
```

### ECS + CloudWatch Logs

- See `infra/ecs-task-def.json` for a task definition using the `awslogs` driver.
- Set `LOG_FORMAT=json` to emit structured JSON logs to stdout; the `awslogs` driver ships them to CloudWatch.
- The container does not need EFS for logs; CloudWatch Logs is recommended. Use EFS only if you require shared file artifacts beyond logs.

### Output line example

```json
{"timestamp":"2025-09-04T01:40:40.529Z","ticker":"TSXV:AXE","news_id":101148083}
```
