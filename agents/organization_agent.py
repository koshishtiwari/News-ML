"""
DEPRECATED: This module has been replaced by the micro-agent architecture.

The functionality formerly provided by this module is now available through:
1. agents.micro_agents.content_analysis_agent
2. core.system.NewsOrchestrator

Please update your code to use these modules instead.
"""

import warnings

# Issue a deprecation warning when imported
warnings.warn(
    "The NewsOrganizationAgent has been replaced by the micro-agent architecture and NewsOrchestrator. "
    "Please use core.system.NewsOrchestrator instead.",
    DeprecationWarning,
    stacklevel=2
)

# For backwards compatibility, we'll create a minimal implementation that warns users
class NewsOrganizationAgent:
    """
    DEPRECATED: This class has been replaced by the NewsOrchestrator in the micro-agent architecture.
    
    Please use core.system.NewsOrchestrator instead.
    """
    
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "NewsOrganizationAgent is deprecated and will be removed in a future version. "
            "Please use core.system.NewsOrchestrator instead.",
            DeprecationWarning,
            stacklevel=2
        )
    
    def __getattr__(self, name):
        """Catch all method calls and warn the user"""
        def _warning_method(*args, **kwargs):
            warnings.warn(
                f"Called '{name}' on deprecated NewsOrganizationAgent. "
                "This class has been replaced by core.system.NewsOrchestrator.",
                DeprecationWarning,
                stacklevel=2
            )
            return None
        return _warning_method