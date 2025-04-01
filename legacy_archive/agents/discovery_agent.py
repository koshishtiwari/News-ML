"""
DEPRECATED: This file has been replaced as part of the migration to the micro-agent architecture.

The functionality provided by this module is now available through the following modules:
1. agents.micro_agents.*
2. core.system.NewsOrchestrator

Please update your imports to use these new modules.
"""

import warnings

warnings.warn(
    "This module has been deprecated and replaced by the micro-agent architecture. "
    "Please update your imports to use agents.micro_agents.* instead.",
    DeprecationWarning,
    stacklevel=2
)

# Import from new module for backwards compatibility
from agents.micro_agents.source_discovery_agent import *
