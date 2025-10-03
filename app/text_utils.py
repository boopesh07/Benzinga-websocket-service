"""Text processing utilities for cleaning and formatting news content.

This module provides utilities to clean HTML content and format text
for optimal LLM and Knowledge Base consumption.
"""

import re
from typing import Optional
import html


def strip_html_tags(html_text: Optional[str]) -> str:
    """Strip HTML tags and return clean text.
    
    Converts HTML to clean, readable text suitable for LLM processing:
    - Removes all HTML tags
    - Unescapes HTML entities (&amp; -> &, &lt; -> <, etc.)
    - Preserves paragraph breaks as double newlines
    - Removes excessive whitespace
    - Handles None/empty input gracefully
    
    Args:
        html_text: Raw HTML text from news article body
        
    Returns:
        Clean text without HTML tags, or empty string if input is None/empty
        
    Examples:
        >>> strip_html_tags("<p>Hello <b>world</b>!</p>")
        "Hello world!"
        
        >>> strip_html_tags("<p>First para</p><p>Second para</p>")
        "First para\\n\\nSecond para"
    """
    if not html_text:
        return ""
    
    text = html_text
    
    # Convert common HTML elements to readable format
    # Preserve paragraph breaks
    text = re.sub(r'</p>\s*<p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>\s*<div>', '\n\n', text, flags=re.IGNORECASE)
    
    # Convert lists to readable format
    text = re.sub(r'<li>', '• ', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    
    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Unescape HTML entities (&amp; -> &, &lt; -> <, etc.)
    text = html.unescape(text)
    
    # Clean up whitespace
    # Replace multiple spaces with single space
    text = re.sub(r' +', ' ', text)
    # Replace multiple newlines with max 2 newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Remove leading/trailing whitespace from each line
    text = '\n'.join(line.strip() for line in text.split('\n'))
    
    # Remove leading/trailing whitespace from entire text
    text = text.strip()
    
    return text

