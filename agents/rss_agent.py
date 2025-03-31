"""
DEPRECATED: This module has been replaced by the micro-agent architecture.

The functionality formerly provided by this module is now available through:
1. agents.micro_agents.rss_discovery_agent
2. agents.micro_agents.content_extraction_agent
3. core.system.NewsOrchestrator

Please update your code to use these modules instead.
"""

import warnings

# Issue a deprecation warning when imported
warnings.warn(
    "The RSSAgent has been replaced by the micro-agent architecture. "
    "Please use agents.micro_agents.rss_discovery_agent instead.",
    DeprecationWarning,
    stacklevel=2
)

# For backwards compatibility, redirect to the new micro-agent
from agents.micro_agents.rss_discovery_agent import RSSDiscoveryAgent

# Alias for backward compatibility
RSSAgent = RSSDiscoveryAgent