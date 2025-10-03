"""Unit tests for WebSocket message processing and summarization.

Tests the complete pipeline from WebSocket message to OutputRecord,
including HTML cleaning, ticker extraction, and Claude summarization.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock
from app.models import (
    StreamMessage,
    DataMessage,
    Content,
    OutputRecord,
    extract_all_outputs,
    try_extract_output,
    _extract_symbol,
)


# ============================================================================
# Module-level fixtures (available to all test classes)
# ============================================================================

@pytest.fixture
def mock_summarizer():
    """Create a mock BedrockSummarizer."""
    summarizer = Mock()
    summarizer.summarize_article.return_value = (
        "Tesla reported Q4 2024 earnings that exceeded Wall Street expectations "
        "by 15%, driven by record vehicle deliveries of 485,000 units. Revenue "
        "reached $25.2 billion with margins improving to 18.3%. The company raised "
        "2025 guidance, citing strong demand and expanded production capacity."
    )
    return summarizer


@pytest.fixture
def sample_message():
    """Create a sample WebSocket message."""
    return {
        "api_version": "websocket/v1",
        "kind": "News/v1",
        "data": {
            "id": 123456,
            "action": "Created",
            "timestamp": "2025-10-02T18:00:00Z",
            "content": {
                "id": 123456,
                "revision_id": 1,
                "type": "article",
                "title": "Tesla Announces Q4 Earnings Beat",
                "body": "<p>Tesla Motors announced today that Q4 earnings <b>exceeded</b> analyst expectations...</p>",
                "teaser": "<p>Tesla beats Q4 estimates with strong deliveries</p>",
                "authors": ["John Doe", "Jane Smith"],
                "url": "https://www.benzinga.com/news/earnings/24/10/tesla-q4-earnings",
                "channels": ["News", "Earnings"],
                "securities": [
                    {"symbol": "TSLA", "exchange": "NASDAQ", "primary": True}
                ],
                "created_at": "2025-10-02T17:55:00Z",
                "updated_at": "2025-10-02T18:00:00Z",
            }
        }
    }


@pytest.fixture
def multi_ticker_message():
    """Create a message with multiple tickers."""
    return {
        "api_version": "websocket/v1",
        "kind": "News/v1",
        "data": {
            "id": 789012,
            "action": "Created",
            "timestamp": "2025-10-02T19:00:00Z",
            "content": {
                "id": 789012,
                "title": "Apple and Microsoft Announce Partnership",
                "body": "<p>Apple Inc. and Microsoft Corporation announced a strategic partnership...</p>",
                "teaser": "<p>AAPL and MSFT to collaborate on AI initiatives</p>",
                "authors": ["Tech Reporter"],
                "url": "https://www.benzinga.com/news/tech/24/10/aapl-msft-partnership",
                "channels": ["News", "Tech"],
                "securities": [
                    {"symbol": "AAPL", "exchange": "NASDAQ"},
                    {"symbol": "MSFT", "exchange": "NASDAQ"}
                ],
                "created_at": "2025-10-02T18:55:00Z",
                "updated_at": "2025-10-02T19:00:00Z",
            }
        }
    }


# ============================================================================
# Test Classes
# ============================================================================

class TestSymbolExtraction:
    """Test ticker symbol extraction from various formats."""
    
    def test_extract_symbol_from_dict(self):
        """Test extracting symbol from dict with 'symbol' key."""
        security = {"symbol": "TSLA", "exchange": "NASDAQ", "primary": True}
        assert _extract_symbol(security) == "TSLA"
    
    def test_extract_symbol_from_string(self):
        """Test extracting symbol from plain string."""
        security = "AAPL"
        assert _extract_symbol(security) == "AAPL"
    
    def test_extract_symbol_from_string_with_whitespace(self):
        """Test extracting symbol from string with whitespace."""
        security = "  MSFT  "
        assert _extract_symbol(security) == "MSFT"
    
    def test_extract_symbol_from_object(self):
        """Test extracting symbol from object with symbol attribute."""
        security = Mock()
        security.symbol = "GOOGL"
        assert _extract_symbol(security) == "GOOGL"
    
    def test_extract_symbol_returns_none_for_empty(self):
        """Test that empty/invalid input returns None."""
        assert _extract_symbol({}) is None
        assert _extract_symbol({"exchange": "NYSE"}) is None
        assert _extract_symbol("") is None
        assert _extract_symbol("   ") is None
        assert _extract_symbol(None) is None


class TestMessageProcessing:
    """Test complete WebSocket message processing pipeline."""
    
    def test_parse_single_ticker_message(self, sample_message, mock_summarizer):
        """Test parsing a message with a single ticker."""
        msg = StreamMessage.model_validate(sample_message)
        records = extract_all_outputs(msg, mock_summarizer)
        
        # Should return exactly one record
        assert len(records) == 1
        
        # Verify all fields are present and correct
        record = records[0]
        assert isinstance(record, OutputRecord)
        assert record.ticker == "TSLA"
        assert record.news_id == 123456
        assert record.action == "Created"
        assert record.title == "Tesla Announces Q4 Earnings Beat"
        assert record.content is not None
        assert "Tesla reported Q4 2024 earnings" in record.content
        assert record.authors == ["John Doe", "Jane Smith"]
        assert record.url == "https://www.benzinga.com/news/earnings/24/10/tesla-q4-earnings"
        assert record.channels == ["News", "Earnings"]
        assert record.timestamp is not None
        assert record.created_at is not None
        assert record.updated_at is not None
    
    def test_parse_multi_ticker_message(self, multi_ticker_message, mock_summarizer):
        """Test parsing a message with multiple tickers."""
        msg = StreamMessage.model_validate(multi_ticker_message)
        records = extract_all_outputs(msg, mock_summarizer)
        
        # Should return two records (one per ticker)
        assert len(records) == 2
        
        # Both records should have same news_id but different tickers
        assert records[0].news_id == records[1].news_id == 789012
        assert records[0].ticker in ["AAPL", "MSFT"]
        assert records[1].ticker in ["AAPL", "MSFT"]
        assert records[0].ticker != records[1].ticker
        
        # Both records should have the same summary (generated once)
        assert records[0].content == records[1].content
        
        # Verify summarizer was called only once
        mock_summarizer.summarize_article.assert_called_once()
    
    def test_html_cleaning_in_content(self, sample_message, mock_summarizer):
        """Test that HTML tags are stripped before summarization."""
        msg = StreamMessage.model_validate(sample_message)
        records = extract_all_outputs(msg, mock_summarizer)
        
        # Get the actual call to summarize_article
        call_args = mock_summarizer.summarize_article.call_args
        
        # Verify HTML was stripped from body
        body_arg = call_args.kwargs['body']
        assert '<p>' not in body_arg
        assert '<b>' not in body_arg
        assert 'exceeded' in body_arg  # Content should still be there
        
        # Verify HTML was stripped from teaser
        teaser_arg = call_args.kwargs['teaser']
        assert '<p>' not in teaser_arg
        assert 'Tesla beats' in teaser_arg
    
    def test_fallback_when_summarization_fails(self, sample_message):
        """Test fallback to cleaned body when Claude API fails."""
        # Create summarizer that returns None (simulating API failure)
        failed_summarizer = Mock()
        failed_summarizer.summarize_article.return_value = None
        
        msg = StreamMessage.model_validate(sample_message)
        records = extract_all_outputs(msg, failed_summarizer)
        
        # Should still return a record
        assert len(records) == 1
        
        # Content should be the cleaned body (truncated to 1000 chars)
        record = records[0]
        assert record.content is not None
        assert '<p>' not in record.content
        assert '<b>' not in record.content
        assert 'exceeded' in record.content.lower()
    
    def test_message_without_securities(self, sample_message, mock_summarizer):
        """Test that message without securities returns empty list."""
        sample_message["data"]["content"]["securities"] = []
        msg = StreamMessage.model_validate(sample_message)
        records = extract_all_outputs(msg, mock_summarizer)
        
        assert len(records) == 0
    
    def test_message_with_empty_symbol(self, sample_message, mock_summarizer):
        """Test handling of securities with empty symbols."""
        sample_message["data"]["content"]["securities"] = [
            {"symbol": "", "exchange": "NASDAQ"},
            {"symbol": "TSLA", "exchange": "NASDAQ"}
        ]
        msg = StreamMessage.model_validate(sample_message)
        records = extract_all_outputs(msg, mock_summarizer)
        
        # Should only return one record (TSLA)
        assert len(records) == 1
        assert records[0].ticker == "TSLA"
    
    def test_mixed_security_formats(self, sample_message, mock_summarizer):
        """Test handling of mixed security formats (dict, string, object)."""
        sample_message["data"]["content"]["securities"] = [
            {"symbol": "TSLA", "exchange": "NASDAQ"},
            "AAPL",  # String format
            {"exchange": "NYSE"},  # No symbol
            "  MSFT  ",  # String with whitespace
        ]
        msg = StreamMessage.model_validate(sample_message)
        records = extract_all_outputs(msg, mock_summarizer)
        
        # Should return 3 records (TSLA, AAPL, MSFT)
        assert len(records) == 3
        tickers = {r.ticker for r in records}
        assert tickers == {"TSLA", "AAPL", "MSFT"}
    
    def test_try_extract_output_returns_first_ticker(self, multi_ticker_message, mock_summarizer):
        """Test that try_extract_output returns only first ticker."""
        msg = StreamMessage.model_validate(multi_ticker_message)
        record = try_extract_output(msg, mock_summarizer)
        
        # Should return only one record (first ticker)
        assert record is not None
        assert isinstance(record, OutputRecord)
        assert record.ticker in ["AAPL", "MSFT"]
    
    def test_output_record_serialization(self, sample_message, mock_summarizer):
        """Test that OutputRecord can be serialized to NDJSON."""
        msg = StreamMessage.model_validate(sample_message)
        records = extract_all_outputs(msg, mock_summarizer)
        
        record = records[0]
        ndjson = record.to_ndjson()
        
        # Verify it's valid JSON with newline
        assert ndjson.endswith('\n')
        
        import json
        data = json.loads(ndjson.strip())
        
        # Verify all expected fields are present
        assert 'ticker' in data
        assert 'news_id' in data
        assert 'action' in data
        assert 'title' in data
        assert 'content' in data
        assert 'authors' in data
        assert 'url' in data
        assert 'channels' in data
        assert 'timestamp' in data
        assert 'created_at' in data
        assert 'updated_at' in data


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_message_with_null_body(self, mock_summarizer):
        """Test handling of message with null body."""
        # Summarizer returns teaser when body is empty
        mock_summarizer.summarize_article.return_value = "Brief teaser content"
        
        msg = StreamMessage.model_validate({
            "data": {
                "id": 1,
                "action": "Created",
                "timestamp": "2025-10-02T18:00:00Z",
                "content": {
                    "title": "Test Article",
                    "body": None,
                    "teaser": "Brief teaser content",
                    "securities": [{"symbol": "TEST"}],
                    "authors": [],
                    "channels": [],
                }
            }
        })
        
        records = extract_all_outputs(msg, mock_summarizer)
        assert len(records) == 1
        assert records[0].content is not None
    
    def test_message_with_null_arrays(self, mock_summarizer):
        """Test handling of null authors and channels."""
        mock_summarizer.summarize_article.return_value = "Summary content"
        
        msg = StreamMessage.model_validate({
            "data": {
                "id": 2,
                "action": "Created",
                "timestamp": "2025-10-02T18:00:00Z",
                "content": {
                    "title": "Test Article",
                    "body": "Content",
                    "securities": [{"symbol": "TEST"}],
                    "authors": None,
                    "channels": None,
                }
            }
        })
        
        records = extract_all_outputs(msg, mock_summarizer)
        assert len(records) == 1
        assert records[0].authors == []
        assert records[0].channels == []
    
    def test_message_with_no_action_field(self, mock_summarizer):
        """Test that action defaults to 'Created' when missing."""
        mock_summarizer.summarize_article.return_value = "Summary content"
        
        msg = StreamMessage.model_validate({
            "data": {
                "id": 3,
                "timestamp": "2025-10-02T18:00:00Z",
                "content": {
                    "title": "Test Article",
                    "body": "Content",
                    "securities": [{"symbol": "TEST"}],
                }
            }
        })
        
        records = extract_all_outputs(msg, mock_summarizer)
        assert len(records) == 1
        assert records[0].action == "Created"


class TestBedrockIntegration:
    """Integration tests that call real AWS Bedrock API.
    
    These tests are marked with @pytest.mark.integration and are skipped by default.
    Run with: pytest -v -m integration
    
    Requirements:
    - AWS credentials configured
    - bedrock:InvokeModel IAM permission
    - AWS_REGION environment variable set
    """
    
    @pytest.mark.integration
    def test_real_claude_summarization(self, sample_message):
        """Test actual Claude API call for summarization.
        
        This test makes a real API call to AWS Bedrock Claude.
        Skip by default to avoid costs and API dependencies.
        
        Run with: pytest -v -m integration
        """
        import os
        from app.bedrock_summarizer import BedrockSummarizer
        
        # Check if AWS credentials are available
        if not os.environ.get("AWS_REGION"):
            pytest.skip("AWS_REGION not set - skipping integration test")
        
        # Create real summarizer
        summarizer = BedrockSummarizer(
            region_name=os.environ.get("AWS_REGION"),
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            max_retries=3,
        )
        
        # Parse message
        msg = StreamMessage.model_validate(sample_message)
        
        # Process with real Claude API
        try:
            records = extract_all_outputs(msg, summarizer)
        except Exception as e:
            pytest.fail(f"Claude API call failed: {e}")
        
        # Verify results
        assert len(records) == 1, "Should return one record"
        record = records[0]
        
        # Verify all fields
        assert record.ticker == "TSLA", "Ticker should be TSLA"
        assert record.news_id == 123456, "News ID should match"
        assert record.action == "Created", "Action should be Created"
        assert record.title == "Tesla Announces Q4 Earnings Beat", "Title should match"
        
        # Verify content (Claude-generated summary)
        assert record.content is not None, "Content should not be None"
        assert len(record.content) > 0, "Content should not be empty"
        assert len(record.content) < 2000, "Content should be reasonably short"
        
        # Verify summary mentions key terms (not HTML)
        content_lower = record.content.lower()
        assert "tesla" in content_lower, "Summary should mention Tesla"
        assert "<p>" not in record.content, "Content should not have HTML tags"
        assert "<b>" not in record.content, "Content should not have HTML tags"
        
        # Verify other fields
        assert record.authors == ["John Doe", "Jane Smith"], "Authors should match"
        assert record.url is not None, "URL should be present"
        assert record.channels == ["News", "Earnings"], "Channels should match"
        
        # Log the actual summary for inspection
        print(f"\n✅ Claude API call successful!")
        print(f"📝 Generated summary ({len(record.content.split())} words):")
        print(f"{record.content}\n")
    
    @pytest.mark.integration
    def test_real_claude_multi_ticker_efficiency(self, multi_ticker_message):
        """Test that Claude is called only once for multi-ticker articles.
        
        This verifies the efficiency optimization where we summarize once
        and reuse the summary for all tickers.
        
        Run with: pytest -v -m integration
        """
        import os
        from app.bedrock_summarizer import BedrockSummarizer
        from unittest.mock import patch
        
        if not os.environ.get("AWS_REGION"):
            pytest.skip("AWS_REGION not set - skipping integration test")
        
        # Create real summarizer
        summarizer = BedrockSummarizer(
            region_name=os.environ.get("AWS_REGION"),
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
            max_retries=3,
        )
        
        # Spy on the summarize_article method
        original_method = summarizer.summarize_article
        call_count = {"count": 0}
        
        def counted_summarize(*args, **kwargs):
            call_count["count"] += 1
            return original_method(*args, **kwargs)
        
        summarizer.summarize_article = counted_summarize
        
        # Parse message with 2 tickers
        msg = StreamMessage.model_validate(multi_ticker_message)
        
        # Process
        records = extract_all_outputs(msg, summarizer)
        
        # Verify efficiency: should call Claude only once
        assert call_count["count"] == 1, "Should call Claude API only once for multi-ticker article"
        
        # Verify output: should have 2 records
        assert len(records) == 2, "Should return 2 records (one per ticker)"
        
        # Verify both records have same summary
        assert records[0].content == records[1].content, "Both tickers should have same summary"
        
        # Verify tickers are different
        tickers = {r.ticker for r in records}
        assert tickers == {"AAPL", "MSFT"}, "Should have both tickers"
        
        print(f"\n✅ Efficiency test passed: 1 API call for 2 tickers")
        print(f"💰 Cost savings: 50% (vs calling API for each ticker)")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

