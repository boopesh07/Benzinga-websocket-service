# Benzinga News WebSocket to S3/File Pipeline

Real-time streaming pipeline that consumes Benzinga Newsfeed WebSocket messages, processes them with **AWS Bedrock Claude 3.5 Sonnet** for intelligent summarization, and writes structured NDJSON records to S3 or local files.

## 🎯 Overview

This service is **optimized for AWS Knowledge Base and RAG (Retrieval Augmented Generation)** workflows, transforming raw HTML news into clean, focused summaries perfect for vector embeddings and LLM queries.

### Key Features

**🤖 AI-Powered Summarization**
- Claude 3.5 Sonnet generates concise 200-word summaries (configurable)
- Extracts key facts, financial impact, and stock implications
- Removes HTML noise and focuses on actionable investor information
- Exponential backoff retry for robust API handling
- Intelligent fallback to cleaned HTML if summarization fails

**📊 Multi-Ticker Support**
- One record per ticker mentioned in each article
- Summary generated once and reused across all tickers (cost-efficient)
- Enables accurate ticker-based queries in Knowledge Base
- Example: "AAPL and MSFT partner" creates 2 records with identical summaries

**🔄 Production-Ready**
- Automatic reconnection with exponential backoff
- WebSocket heartbeat monitoring (ping/pong)
- Graceful shutdown handling (SIGINT/SIGTERM)
- Time-based windowed partitioning (configurable: 15/30/60 min)
- Size-based file rotation with configurable limits
- S3 multipart uploads with progress markers

**🚀 Flexible Deployment**
- File sink for local development/testing
- S3 sink for production deployment
- Docker container with ECS support
- CloudWatch Logs integration with structured JSON logging
- IAM role-based AWS authentication

## 📋 Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Output Format](#output-format)
- [Deployment](#deployment)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## 🏗️ Architecture

### Pipeline Flow

```
┌─────────────────┐
│  Benzinga WS    │  Raw news with HTML content
│  (Real-time)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  WebSocket      │  • Auto-reconnect with backoff
│  Client         │  • Ping/pong heartbeat
│                 │  • JSON parsing & validation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  HTML Cleaner   │  Strip tags, unescape entities
│  (text_utils)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Claude 3.5     │  • Generate 200-word summary
│  Sonnet         │  • Focus on financial impact
│  (Bedrock)      │  • Retry with exponential backoff
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Record         │  • One record per ticker
│  Generator      │  • Reuse summary for all tickers
│                 │  • Fallback to clean HTML
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Windowed       │  • Time-based partitioning
│  Writer         │  • Size-based rotation
│  (S3/File)      │  • Date-based folder structure
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  S3 Bucket      │  s3://bucket/prefix/ingest_dt=YYYY/MM/DD/hour=HH/part=N/*.ndjson
│  (Knowledge     │
│   Base Sync)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  OpenSearch     │  Vector database with embeddings
│  (Vector DB)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AWS Knowledge  │  RAG queries: "What's the latest TSLA news?"
│  Base + Claude  │
└─────────────────┘
```

### Why Claude Summarization?

| Aspect | Before (Raw HTML) | After (Claude Summary) |
|--------|------------------|----------------------|
| **Size** | ~5-10KB per record | ~1-2KB per record (80% reduction) |
| **Readability** | HTML tags, formatting | Clean, focused prose |
| **Embedding Quality** | Noisy signals | Dense, semantic content |
| **Query Accuracy** | Misses key facts in HTML | Highlights financial impact |
| **Storage Costs** | High | Low (5x reduction) |
| **Knowledge Base** | Poor retrieval | Excellent retrieval |

## 🚀 Quick Start

### Local Development (File Sink)

```bash
# 1. Clone and setup
git clone <repo-url>
cd Benzinga-websocket-service
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp env.example .env
# Edit .env and set BENZINGA_API_KEY and AWS credentials

# 4. Run with file sink (no S3 required)
export SINK=file
export FILE_DIR=./data
python -m app.main
```

Output files appear under `./data/ingest_dt=YYYY/MM/DD/hour=HH/part=N/*.ndjson`

### Production (S3 Sink)

```bash
# Configure S3
export SINK=s3
export S3_BUCKET=your-bucket-name
export S3_PREFIX=benzinga/news
export AWS_REGION=us-east-1

# AWS credentials via environment or IAM role
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Run
python -m app.main
```

## ⚙️ Configuration

All settings are configured via environment variables or `.env` file.

### Required Settings

```bash
# Benzinga API
BENZINGA_API_KEY=your_api_key_here    # Required: Benzinga API key
```

### API Settings

```bash
BENZINGA_WS_URL=wss://api.benzinga.com/api/v1/news/stream  # WebSocket URL (default shown)
```

### Output Sink Configuration

```bash
# Sink selection
SINK=file                              # Output destination: 'file' or 's3' (default: file)

# File sink settings
FILE_DIR=./data                        # Base directory for file sink (default: ./data)

# S3 sink settings (required when SINK=s3)
S3_BUCKET=your-bucket-name             # S3 bucket name
S3_PREFIX=benzinga/news                # S3 key prefix (default: benzinga/news)
AWS_REGION=us-east-1                   # AWS region (optional, uses credential chain default)
```

### AWS Bedrock Settings (Article Summarization)

```bash
# Model configuration
BEDROCK_MODEL_ID=us.anthropic.claude-3-5-sonnet-20241022-v2:0  # Claude model ID (inference profile)
SUMMARY_MAX_WORDS=200                  # Max words in summary (shorter OK, default: 200)
BEDROCK_MAX_RETRIES=3                  # Max retry attempts for API calls (default: 3)

# Note: Uses AWS_REGION for Bedrock (same as S3)
```

**Claude Model Options:**
- `us.anthropic.claude-3-5-sonnet-20241022-v2:0` - Best quality (recommended, us-east-1)
- `us-west-2.anthropic.claude-3-5-sonnet-20241022-v2:0` - Best quality (us-west-2)
- `anthropic.claude-3-haiku-20240307-v1:0` - Faster, cheaper (if cost is a concern)

**Note:** For Claude 3.5 Sonnet on-demand access, use region-specific inference profiles (e.g., `us.anthropic.claude-3-5-sonnet-20241022-v2:0` for us-east-1). If the primary model fails due to configuration issues, the system will automatically fall back to Claude 3 Haiku.

The migration script now handles malformed JSON/HTML by passing raw content directly to the LLM, which is more reliable than attempting to fix parsing issues on our side.

### Windowed Writer Settings

```bash
WINDOW_MINUTES=30                      # Time window size in minutes (default: 30)
                                       # Recommended: 15, 30, or 60
MAX_OBJECT_BYTES=512000000             # Max file/object size before rotation (default: 512MB)
PART_SIZE_BYTES=16777216               # S3 multipart part size (default: 16MB, min: 5MB)
USE_MARKER_FILES=true                  # Create uploading.marker files (default: true)
```

**Partitioning Strategy:**
- Files are organized by date: `ingest_dt=YYYY/MM/DD/hour=HH/part=N/`
- Each window creates a new file every `WINDOW_MINUTES`
- Files rotate early if they exceed `MAX_OBJECT_BYTES`
- Part numbers increment within each hour

### WebSocket Settings

```bash
PING_INTERVAL=30                       # WebSocket ping interval in seconds (default: 30)
PING_TIMEOUT=10                        # WebSocket ping timeout in seconds (default: 10)
```

### Reconnection Settings

```bash
RECONNECT_BASE_DELAY=1.0               # Initial reconnection delay in seconds (default: 1.0)
RECONNECT_MAX_DELAY=60.0               # Maximum reconnection delay in seconds (default: 60.0)
```

### Logging

```bash
LOG_LEVEL=INFO                         # Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT=text                        # Log format: 'text' (human) or 'json' (structured)
```

**Log Format Recommendations:**
- **Local development**: `LOG_FORMAT=text` (easier to read)
- **Production/ECS**: `LOG_FORMAT=json` (structured, machine-parseable)

## 📄 Output Format

Each NDJSON record contains a **Claude-generated summary** optimized for embeddings and vector search.

### Output Schema

```json
{
  "timestamp": 1759497640529,                 // Event timestamp (Epoch milliseconds)
  "ticker": "TSLA",                             // Stock ticker symbol
  "news_id": 101148083,                         // Unique news article ID
  "action": "Created",                          // Article action (Created, Updated, etc.)
  "title": "Tesla Announces Q4 Earnings Beat",  // Article title
  "content": "Claude-generated summary...",     // ~200 word summary (key field!)
  "authors": ["John Doe", "Jane Smith"],        // List of authors
  "url": "https://www.benzinga.com/...",        // Article URL
  "channels": ["News", "Earnings"],             // List of channel tags
  "created_at": "2025-10-02T17:55:00Z",         // Article creation timestamp
  "updated_at": "2025-10-02T18:00:00Z"          // Article update timestamp
}
```

### Example Output Record

```json
{
  "timestamp": 1759497640529,
  "ticker": "TSLA",
  "news_id": 101148083,
  "action": "Created",
  "title": "Tesla Announces Record Q4 2024 Earnings",
  "content": "Tesla reported Q4 2024 earnings that exceeded Wall Street expectations by 15%, driven by record vehicle deliveries of 485,000 units, up 20% year-over-year. Revenue reached $25.2 billion with gross margins improving to 18.3%. CEO Elon Musk highlighted successful production scaling at Austin and Berlin factories, with both facilities now operating at full capacity. The company raised 2025 delivery guidance to 2.1 million vehicles, citing strong demand in China and Europe alongside expanded production capacity. Energy storage revenue grew 60% year-over-year to $3.2 billion. Operating cash flow improved to $4.1 billion. Several analysts upgraded price targets following the results, with Deutsche Bank raising its target to $300. Stock implications suggest continued growth momentum, operational efficiency gains, and potential market share expansion in the EV sector.",
  "authors": ["John Doe", "Jane Smith"],
  "url": "https://www.benzinga.com/news/earnings/24/10/tesla-q4-earnings",
  "channels": ["News", "Earnings"],
  "created_at": "2025-10-02T17:55:00Z",
  "updated_at": "2025-10-02T18:00:00Z"
}
```

### Content Field Details

The `content` field contains the **Claude-generated summary** with:
- ✅ **Key financial metrics** (revenue, earnings, percentages)
- ✅ **Specific numbers and dates** (deliveries, targets, quarters)
- ✅ **Management commentary** (CEO statements, guidance)
- ✅ **Market reaction** (analyst ratings, price targets)
- ✅ **Stock implications** (growth outlook, competitive position)
- ❌ **No HTML tags** (clean text only)
- ❌ **No meta-commentary** (straight to the facts)

### Multi-Ticker Articles

When an article mentions multiple tickers:

**Input:**
```json
{
  "title": "Apple and Microsoft Announce AI Partnership",
  "securities": [
    {"symbol": "AAPL"},
    {"symbol": "MSFT"}
  ]
}
```

**Output:** 2 records with **identical summaries**
```json
{"ticker": "AAPL", "content": "Apple Inc. and Microsoft Corporation announced...", ...}
{"ticker": "MSFT", "content": "Apple Inc. and Microsoft Corporation announced...", ...}
```

**Benefits:**
- ✅ Query "AAPL news" → Returns this article
- ✅ Query "MSFT news" → Also returns this article
- ✅ Summary generated **once** → Cost-efficient (1 API call for N tickers)

## 🔄 Data Migration

A migration script is available to update the timestamp format in existing `.ndjson` files from ISO 8601 strings to epoch milliseconds.

The script is optimized for performance and safety:
- **Parallel Processing**: It processes multiple S3 files and multiple records within each file concurrently to speed up migration.
- **S3 Rate Limiting**: It uses `boto3`'s adaptive retry mode, which automatically adjusts request rates to avoid S3 throttling errors.

### How to Run

```bash
# Ensure you have AWS credentials configured in your environment
# Example: s3://your-bucket/benzinga/news/
python migrate_timestamps.py s3://<your-bucket>/<your-prefix>/ --concurrency 10
```

- `s3_path`: The S3 path containing the `.ndjson` files to migrate.
- `--concurrency`: (Optional) The number of parallel workers to use. Defaults to 5.

## 🐳 Docker

### Build and Run

```bash
# Build image
docker build -t bz-ws-sink .

# Run with file sink (mount local directory)
docker run --rm \
  -e SINK=file \
  -e FILE_DIR=/data \
  -e BENZINGA_API_KEY=your_key_here \
  -e AWS_REGION=us-east-1 \
  -v $(pwd)/data:/data \
  bz-ws-sink

# Run with S3 sink (use .env file for secrets)
docker run --rm --env-file .env bz-ws-sink
```

### Environment Variables in Docker

Create a `.env` file:
```bash
BENZINGA_API_KEY=your_key_here
SINK=s3
S3_BUCKET=your-bucket
S3_PREFIX=benzinga/news
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
LOG_FORMAT=json
LOG_LEVEL=INFO
```

## ☁️ Deployment

### Prerequisites

1. **AWS Resources:**
   - S3 bucket for output
   - IAM execution role with ECR, CloudWatch Logs permissions
   - IAM task role with S3, Bedrock permissions
   - ECS cluster (Fargate)
   - VPC with subnet and security group

2. **Required IAM Permissions:**

**Task Role** (for application):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket/*",
        "arn:aws:s3:::your-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:*::foundation-model/anthropic.claude-*"
    }
  ]
}
```

**Execution Role** (for ECS):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

### Build and Deploy to ECS

1. **Configure `.env.prod`:**

```bash
# AWS
ACCOUNT_ID=123456789012
AWS_REGION=us-east-1

# Benzinga
BENZINGA_API_KEY=your_api_key_here

# S3
S3_BUCKET=your-bucket-name
S3_PREFIX=benzinga/news

# Bedrock (optional, uses defaults if not set)
BEDROCK_MODEL_ID=us.anthropic.claude-3-5-sonnet-20241022-v2:0
SUMMARY_MAX_WORDS=200
BEDROCK_MAX_RETRIES=3

# Logging
LOG_FORMAT=json
LOG_LEVEL=INFO

# ECS
CLUSTER=etl-pipeline-cluster
SUBNET_ID=subnet-xxxxx
SECURITY_GROUP_ID=sg-xxxxx
```

2. **Build and push Docker image:**

```bash
./build.sh
```

This script:
- Authenticates with ECR
- Builds linux/amd64 image using buildx
- Pushes to ECR: `{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/bz-ws-sink:latest`

3. **Deploy to ECS:**

```bash
./deploy.sh
```

This script:
- Renders `infra/ecs-task-def.json` with environment variables
- Registers new ECS task definition
- Launches Fargate task with specified networking

### CloudWatch Logs

The ECS task definition uses the `awslogs` driver to stream logs to CloudWatch Logs.

**Log Group:** `/ecs/etl-pipeline-logs`  
**Log Stream:** `ecs/{container-name}/{task-id}`

**View logs:**
```bash
aws logs tail /ecs/etl-pipeline-logs --follow
```

**Structured JSON logs** (when `LOG_FORMAT=json`):
```json
{
  "timestamp": "2025-10-02T18:40:50Z",
  "level": "INFO",
  "logger": "app.ws_client",
  "message": "writing record",
  "news_id": 101148083,
  "ticker": "TSLA"
}
```

## 🧪 Testing

### Unit Tests (Default)

Unit tests use **mocked** Claude API (no AWS costs):

```bash
# Install dependencies
pip install -r requirements.txt

# Run unit tests (integration tests skipped by default)
pytest -v

# Run specific test file
pytest tests/test_message_processing.py -v

# With coverage report
pytest --cov=app --cov-report=html
open htmlcov/index.html
```

**Test Coverage:**
- ✅ Symbol extraction (dict, string, object formats)
- ✅ HTML cleaning and text processing
- ✅ Multi-ticker article handling
- ✅ Summarization with fallback logic
- ✅ Empty/null field handling
- ✅ NDJSON serialization

### Integration Tests (Real Claude API)

Integration tests make **real AWS Bedrock API calls** (⚠️ incurs costs):

```bash
# Set AWS credentials
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Run ONLY integration tests
pytest -v -m integration

# Run ALL tests (unit + integration)
pytest -v -m ""
```

**Requirements:**
- AWS credentials configured
- `bedrock:InvokeModel` IAM permission
- ⚠️ Costs: ~$0.01-0.02 per test run

**Integration tests verify:**
- ✅ Real Claude API calls work correctly
- ✅ Summaries contain no HTML tags
- ✅ Summaries mention key terms from article
- ✅ Multi-ticker efficiency (1 API call for N tickers)
- ✅ Exponential backoff retry logic

### Test Configuration

**pytest.ini:**
```ini
[pytest]
testpaths = tests
addopts = -v --strict-markers --tb=short -m "not integration"
markers =
    unit: Unit tests (default)
    integration: Integration tests requiring AWS (skipped by default)
```

## 📊 Monitoring

### Key Metrics to Monitor

1. **WebSocket Health:**
   - Connection uptime
   - Reconnection frequency
   - Message processing rate

2. **Claude API:**
   - Summarization success rate
   - API latency (p50, p99)
   - Throttling/error rate
   - Retry counts

3. **Output Pipeline:**
   - Records written per minute
   - File rotation frequency
   - S3 upload success rate
   - Data lag (event time vs. processing time)

4. **Costs:**
   - Bedrock API usage (input/output tokens)
   - S3 storage and requests
   - Data transfer

### Log Analysis

**Successful summarization:**
```
INFO  summarized ticker=TSLA words=195 attempt=1
DEBUG writing record news_id=101148083 ticker=TSLA
```

**Throttling with retry:**
```
WARN  bedrock-throttled ticker=AAPL attempt=1 retrying_in=1s
INFO  summarized ticker=AAPL words=203 attempt=2
```

**Fallback to cleaned HTML:**
```
WARN  summarization-failed news_id=101148083 using-body-fallback
DEBUG writing record news_id=101148083 ticker=TSLA
```

**Multi-ticker optimization:**
```
DEBUG multi-ticker-article news_id=789012 tickers=2
DEBUG writing record news_id=789012 ticker=AAPL
DEBUG writing record news_id=789012 ticker=MSFT
```

### CloudWatch Insights Queries

**Summarization success rate:**
```sql
fields @timestamp, news_id, ticker
| filter message like "summarized" or message like "summarization-failed"
| stats count(*) as total by message
```

**API retry analysis:**
```sql
fields @timestamp, ticker, attempt
| filter message = "bedrock-throttled"
| stats count(*) as retries, avg(attempt) as avg_attempt by ticker
```

**Multi-ticker articles:**
```sql
fields @timestamp, news_id, tickers
| filter message = "multi-ticker-article"
| stats count(*) as count, avg(tickers) as avg_tickers, max(tickers) as max_tickers
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Claude API Throttling

**Symptom:**
```
WARN bedrock-throttled ticker=TSLA attempt=1 retrying_in=1s
```

**Solutions:**
- ✅ Increase `BEDROCK_MAX_RETRIES` (default: 3)
- ✅ Request quota increase from AWS Support
- ✅ Switch to Claude Haiku (faster, higher quota)
- ✅ Implement batching (future enhancement)

#### 2. S3 Upload Failures

**Symptom:**
```
ERROR Failed to upload S3 object: AccessDenied
```

**Solutions:**
- ✅ Verify IAM task role has `s3:PutObject` permission
- ✅ Check S3 bucket policy allows writes
- ✅ Verify bucket exists and region is correct

#### 3. WebSocket Disconnections

**Symptom:**
```
WARN ws-error error=ConnectionClosed reconnecting_in=2.5s
```

**Solutions:**
- ✅ Normal behavior, automatic reconnection enabled
- ✅ Check network connectivity
- ✅ Verify Benzinga API key is valid
- ✅ Increase `RECONNECT_MAX_DELAY` if needed

#### 4. Empty Summaries

**Symptom:**
```
WARN empty-body ticker=TSLA
WARN summarization-failed news_id=123 using-body-fallback
```

**Solutions:**
- ✅ Fallback automatically uses cleaned body text
- ✅ Verify article actually has content
- ✅ Check if Benzinga API is sending empty bodies

### Debug Mode

Enable detailed logging:

```bash
export LOG_LEVEL=DEBUG
python -m app.main
```

Debug logs include:
- Full WebSocket message payloads
- HTML cleaning results
- Claude prompt and response
- Record field values

## 📚 API Reference

### Benzinga WebSocket API

**Documentation:** https://docs.benzinga.com/benzinga-apis/newsfeed-stream-v1/get-stream

**Message Format:**
```json
{
  "api_version": "websocket/v1",
  "kind": "News/v1",
  "data": {
    "id": 123456,
    "action": "Created",
    "timestamp": "2025-10-02T18:00:00Z",
    "content": {
      "title": "...",
      "body": "<p>HTML content...</p>",
      "teaser": "...",
      "securities": [
        {"symbol": "TSLA", "exchange": "NASDAQ"},
        "AAPL"  // Can be string or object
      ],
      "authors": ["..."],
      "channels": ["..."],
      "url": "...",
      "created_at": "...",
      "updated_at": "..."
    }
  }
}
```

### AWS Bedrock Claude Models

**Supported Models:**
- `anthropic.claude-3-5-sonnet-20241022-v2:0` - Latest, best quality
- `anthropic.claude-3-haiku-20240307-v1:0` - Fast, cost-effective

**Pricing (as of Oct 2024):**
- **Claude 3.5 Sonnet:** $3.00 per 1M input tokens, $15.00 per 1M output tokens
- **Claude 3 Haiku:** $0.25 per 1M input tokens, $1.25 per 1M output tokens

**Estimated Costs (200-word summaries):**
- Average article: ~1000 tokens input, ~250 tokens output
- **Sonnet:** ~$0.007 per article
- **Haiku:** ~$0.0006 per article (12x cheaper)

## 🤝 Contributing

### Development Setup

```bash
# Clone repository
git clone <repo-url>
cd Benzinga-websocket-service

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies with dev tools
pip install -r requirements.txt

# Run tests
pytest -v

# Run with file sink
export SINK=file
export BENZINGA_API_KEY=your_key
python -m app.main
```

### Code Style

- Python 3.9+
- Type hints for all function signatures
- Docstrings for all modules, classes, and functions
- PEP 8 style guide
- Black formatter (line length: 100)

## 📝 License

[Your License Here]

## 🆘 Support

For issues or questions:
- GitHub Issues: [Your Repo Issues]
- Email: [Your Email]
- Slack: [Your Slack Channel]

## 🔗 Resources

- [Benzinga Newsfeed API Docs](https://docs.benzinga.com/benzinga-apis/newsfeed-stream-v1)
- [AWS Bedrock Claude Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages.html)
- [AWS S3 Multipart Upload Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [ECS Fargate Best Practices](https://docs.aws.amazon.com/AmazonECS/latest/bestpracticesguide/intro.html)
