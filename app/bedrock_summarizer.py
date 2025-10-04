"""AWS Bedrock integration for article summarization using Claude.

This module provides article summarization using AWS Bedrock Claude models
with exponential backoff retry for robustness.
"""

import json
import logging
import time
from typing import Optional
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class BedrockSummarizer:
    """Summarize news articles using AWS Bedrock Claude.
    
    Features:
    - Exponential backoff retry for API failures
    - Configurable model and word limits
    - Robust error handling with fallbacks
    """
    
    def __init__(
        self,
        region_name: Optional[str] = None,
        model_id: str = "anthropic.claude-3-5-sonnet-20241022-v2:0",
        fallback_model_id: str = "anthropic.claude-3-haiku-20240307-v1:0",
        max_retries: int = 3,
    ):
        """Initialize Bedrock client for Claude models.

        For Claude 3.5 Sonnet on-demand access, use inference profile:
        - us-east-1: us.anthropic.claude-3-5-sonnet-20241022-v2:0
        - us-west-2: us-west-2.anthropic.claude-3-5-sonnet-20241022-v2:0

        Args:
            region_name: AWS region (uses default if not specified)
            model_id: Claude model ID (default: Claude 3.5 Sonnet)
            fallback_model_id: Fallback model ID to use if primary fails
            max_retries: Maximum retry attempts (default: 3)
        """
        self.bedrock = boto3.client("bedrock-runtime", region_name=region_name)
        self.model_id = model_id
        self.region_name = region_name
        self.max_retries = max_retries
        self.fallback_model_id = fallback_model_id

        logger.info("initialized bedrock summarizer model=%s region=%s", model_id, region_name or "default")
    
    def _invoke_with_retry(
        self,
        prompt: str,
        max_words: int,
        ticker: str,
        log_context: str,
    ) -> Optional[str]:
        """Invoke Bedrock with exponential backoff and fallback model logic."""
        for attempt in range(self.max_retries):
            try:
                summary = self._invoke_bedrock_with_model(prompt, max_words, self.model_id)
                if summary:
                    logger.debug(
                        "%s-summarized ticker=%s words=%d attempt=%d",
                        log_context,
                        ticker,
                        len(summary.split()),
                        attempt + 1,
                    )
                    return summary
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                error_message = e.response.get("Error", {}).get("Message", "")

                is_retryable = error_code in (
                    "ThrottlingException",
                    "ServiceUnavailableException",
                    "TooManyRequestsException"
                ) and "ValidationException" not in error_code

                if not is_retryable or attempt == self.max_retries - 1:
                    if error_code == "ValidationException" and "on-demand throughput" in error_message:
                        logger.warning(
                            "Primary model %s not available, trying fallback model %s",
                            self.model_id, self.fallback_model_id
                        )
                        return self._invoke_bedrock_with_model(prompt, max_words, self.fallback_model_id)

                    logger.error(
                        "bedrock-error ticker=%s error_code=%s error_message=%s attempt=%d",
                        ticker, error_code, error_message, attempt + 1
                    )
                    return None
                
                wait_time = 2 ** attempt
                logger.warning(
                    "bedrock-throttled ticker=%s attempt=%d retrying_in=%ds",
                    ticker, attempt + 1, wait_time
                )
                time.sleep(wait_time)
                
            except Exception:
                logger.exception("%s-summarization-failed ticker=%s attempt=%d", log_context, ticker, attempt + 1)
                
                if attempt == self.max_retries - 1:
                    return None
                
                time.sleep(2 ** attempt)
        
        return None

    def summarize_article(
        self,
        ticker: str,
        title: str,
        body: str,
        teaser: Optional[str] = None,
        max_words: int = 200,
    ) -> Optional[str]:
        """Summarize article using Claude with exponential backoff retry.
        
        Args:
            ticker: Stock ticker symbol
            title: Article title
            body: Clean article body text (HTML already stripped)
            teaser: Article teaser (optional)
            max_words: Maximum words in summary (not required if content is shorter)
            
        Returns:
            Summary text or None if all retries fail
        """
        if not body or not body.strip():
            logger.warning("empty-body ticker=%s", ticker)
            return teaser if teaser else None
        
        prompt = self._build_prompt(ticker, title, body, teaser, max_words)
        return self._invoke_with_retry(prompt, max_words, ticker, "article")
    
    def _invoke_bedrock_with_model(self, prompt: str, max_words: int, model_id: str) -> Optional[str]:
        """Invoke Bedrock API with specific model.

        Args:
            prompt: Complete prompt text
            max_words: Maximum words in output
            model_id: Model ID to use for invocation

        Returns:
            Summary text or None if invocation fails
        """
        # Calculate max tokens (roughly words * 1.3 for safety)
        max_tokens = int(max_words * 1.3) + 100  # Add buffer
        
        response = self.bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": 0.3,  # Lower temperature for more focused summaries
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            })
        )
        
        # Parse response
        response_body = json.loads(response["body"].read())
        summary = response_body["content"][0]["text"].strip()
        
        return summary if summary else None

    def summarize_html_content(
        self,
        ticker: str,
        title: str,
        html_content: str,
        max_words: int = 200,
    ) -> Optional[str]:
        """Summarize HTML content directly without preprocessing.

        This method passes raw HTML content to the LLM and instructs it to handle
        HTML parsing and summarization, which is more reliable than trying to
        fix JSON/HTML parsing issues on our side.

        Args:
            ticker: Stock ticker symbol
            title: Article title
            html_content: Raw HTML content from the article body
            max_words: Maximum words in summary (default: 200)

        Returns:
            Summary text or None if all retries fail
        """
        if not html_content or not html_content.strip():
            logger.warning("empty-html-content ticker=%s", ticker)
            return None

        prompt = self._build_html_summary_prompt(ticker, title, html_content, max_words)
        return self._invoke_with_retry(prompt, max_words, ticker, "html")

    def _invoke_bedrock(self, prompt: str, max_words: int) -> Optional[str]:
        """Invoke Bedrock API with primary model (backward compatibility)."""
        return self._invoke_bedrock_with_model(prompt, max_words, self.model_id)

    def _build_prompt(
        self,
        ticker: str,
        title: str,
        body: str,
        teaser: Optional[str],
        max_words: int,
    ) -> str:
        """Build summarization prompt for Claude.
        
        Args:
            ticker: Stock ticker
            title: Article title
            body: Clean body text
            teaser: Optional teaser
            max_words: Max words in summary
            
        Returns:
            Complete prompt string
        """
        parts = [
            f"Summarize this financial news article about {ticker}.",
            "",
            f"Title: {title}",
        ]
        
        if teaser and teaser.strip():
            parts.append(f"Teaser: {teaser}")
        
        parts.extend([
            "",
            "Article:",
            body,
            "",
            "Requirements:",
            f"- Maximum {max_words} words (shorter is fine if content is brief)",
            "- Focus on key facts, financial impact, and stock implications",
            "- Include specific numbers, percentages, and dates when relevant",
            "- Write in clear, professional financial journalism style",
            "- Start directly with the news content (no meta-commentary)",
            "- Prioritize actionable information for investors",
        ])
        
        return "\n".join(parts)

    def _build_html_summary_prompt(
        self,
        ticker: str,
        title: str,
        html_content: str,
        max_words: int,
    ) -> str:
        """Build summarization prompt for HTML content.

        Args:
            ticker: Stock ticker
            title: Article title
            html_content: Raw HTML content
            max_words: Max words in summary

        Returns:
            Complete prompt string for HTML summarization
        """
        parts = [
            f"Summarize this financial news article about {ticker} that is provided in raw HTML format.",
            "",
            f"Title: {title}",
            "",
            "HTML Content:",
            html_content,
            "",
            "Requirements:",
            f"- Maximum {max_words} words (shorter is fine if content is brief)",
            "- Extract and summarize the main article content from the HTML",
            "- Focus on key facts, financial impact, and stock implications",
            "- Include specific numbers, percentages, and dates when relevant",
            "- Write in clear, professional financial journalism style",
            "- Start directly with the news content (no meta-commentary)",
            "- Prioritize actionable information for investors",
            "- Ignore any HTML tags, formatting, or navigation elements",
        ]

        return "\n".join(parts)

