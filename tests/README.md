# Tests

Unit tests for the Benzinga WebSocket service.

## Running Tests

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run Unit Tests (Default)

```bash
# From project root - runs only unit tests (skips integration)
pytest

# With verbose output
pytest -v

# With coverage
pytest --cov=app --cov-report=html
```

### Run Integration Tests (Real Claude API)

Integration tests make real AWS Bedrock API calls and are **skipped by default** to avoid costs.

```bash
# Set AWS credentials
export AWS_REGION=us-east-1

# Run ONLY integration tests
pytest -v -m integration

# Run ALL tests (unit + integration)
pytest -v -m ""
```

**Note:** Integration tests require:
- AWS credentials configured
- `bedrock:InvokeModel` IAM permission
- `AWS_REGION` environment variable
- Will incur AWS Bedrock costs (~$0.01 per test)

### Run Specific Test File

```bash
pytest tests/test_message_processing.py -v
```

### Run Specific Test

```bash
pytest tests/test_message_processing.py::TestMessageProcessing::test_parse_single_ticker_message -v
```

## Test Coverage

### `test_message_processing.py`

**Test Classes:**
1. `TestSymbolExtraction` - Tests for ticker symbol extraction from various formats
2. `TestMessageProcessing` - Tests for complete WebSocket message processing pipeline
3. `TestEdgeCases` - Tests for edge cases and error handling

**What's Tested:**
- ✅ Symbol extraction from dict, string, and object formats
- ✅ Single ticker message parsing
- ✅ Multi-ticker message parsing (one record per ticker)
- ✅ HTML cleaning before summarization
- ✅ Fallback when Claude API fails
- ✅ Messages without securities
- ✅ Empty or invalid symbols
- ✅ Mixed security formats
- ✅ NDJSON serialization
- ✅ Null/empty fields handling
- ✅ Default action field

**Test Fixtures:**
- `mock_summarizer` - Mocked BedrockSummarizer (avoids real API calls)
- `sample_message` - Single ticker WebSocket message
- `multi_ticker_message` - Multi-ticker WebSocket message

## Writing New Tests

### Example Test

```python
def test_new_feature(sample_message, mock_summarizer):
    """Test description."""
    msg = StreamMessage.model_validate(sample_message)
    records = extract_all_outputs(msg, mock_summarizer)
    
    assert len(records) > 0
    assert records[0].ticker == "TSLA"
```

### Best Practices

1. **Use fixtures** for common test data
2. **Mock external dependencies** (BedrockSummarizer, AWS APIs)
3. **Test happy path and edge cases**
4. **Use descriptive test names** (test_what_is_being_tested)
5. **Assert all important fields** in output
6. **Keep tests isolated** (no shared state)

## CI/CD Integration

Add to your CI pipeline:

```yaml
# .github/workflows/test.yml
- name: Run tests
  run: |
    pip install -r requirements.txt
    pytest tests/ -v --cov=app
```

