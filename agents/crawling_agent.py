"""
DEPRECATED: This module has been replaced by the micro-agent architecture.

The functionality formerly provided by this module is now available through:
1. agents.micro_agents.content_extraction_agent
2. core.system.NewsOrchestrator

Please update your code to use these modules instead.
"""

import warnings

# Issue a deprecation warning when imported
warnings.warn(
    "The NewsCrawlingAgent has been replaced by ContentExtractionAgent in the micro-agent architecture. "
    "Please use agents.micro_agents.content_extraction_agent instead.",
    DeprecationWarning,
    stacklevel=2
)

# For backwards compatibility, redirect to the new micro-agent
from agents.micro_agents.content_extraction_agent import ContentExtractionAgent

# Alias for backward compatibility
NewsCrawlingAgent = ContentExtractionAgent