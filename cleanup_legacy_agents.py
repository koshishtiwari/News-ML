#!/usr/bin/env python3
"""
Script to clean up the project by archiving legacy agent files and updating references.
This helps complete the transition to the micro-agent architecture.
"""

import os
import shutil
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Paths
project_root = Path(__file__).parent
legacy_agents_dir = project_root / "agents"
micro_agents_dir = legacy_agents_dir / "micro_agents"
archive_dir = project_root / "legacy_archive"

# Legacy files to archive (these are replaced by micro-agents)
legacy_files = [
    "agents/analysis_agent.py",
    "agents/crawling_agent.py", 
    "agents/discovery_agent.py",
    "agents/organization_agent.py",
    "agents/rss_agent.py",
]

def create_archive_dir():
    """Create archive directory if it doesn't exist"""
    if not archive_dir.exists():
        os.makedirs(archive_dir, exist_ok=True)
        logger.info(f"Created archive directory: {archive_dir}")
    
    # Create agents subdirectory in archive
    agents_archive = archive_dir / "agents"
    if not agents_archive.exists():
        os.makedirs(agents_archive, exist_ok=True)

def archive_legacy_files():
    """Move legacy agent files to archive directory"""
    create_archive_dir()
    
    for file_path in legacy_files:
        src = project_root / file_path
        dest = archive_dir / file_path
        
        # Create parent directories if needed
        os.makedirs(dest.parent, exist_ok=True)
        
        if src.exists():
            shutil.copy2(src, dest)
            logger.info(f"Archived {file_path} to {dest}")
        else:
            logger.warning(f"File {file_path} not found, skipping")

def create_migration_notice():
    """Create migration notice files that explain the transition"""
    for file_path in legacy_files:
        src = project_root / file_path
        if src.exists():
            with open(src, 'w') as f:
                f.write(f'''"""
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
from agents.micro_agents.{"content_analysis_agent" if "analysis" in file_path else 
                           "content_extraction_agent" if "crawling" in file_path else
                           "source_discovery_agent" if "discovery" in file_path else
                           "rss_discovery_agent" if "rss" in file_path else
                           "content_analysis_agent" # default case
                          } import *
''')
            logger.info(f"Created migration notice in {file_path}")

def update_readme():
    """Update the README to reflect the new architecture"""
    readme_path = project_root / "README.md"
    if not readme_path.exists():
        logger.warning("README.md not found, skipping update")
        return
    
    with open(readme_path, 'r') as f:
        content = f.read()
    
    # Look for a section to update
    if "## Architecture" in content or "## Components" in content:
        # Already has section, update it
        logger.info("README.md already has architecture section")
    else:
        # Add architecture section
        architecture_section = """
## Architecture

This project uses a micro-agent architecture where specialized agents handle specific tasks:

- **Source Discovery Agent**: Identifies news sources for a location
- **RSS Discovery Agent**: Finds and processes RSS feeds
- **Content Extraction Agent**: Extracts and cleans article content
- **Content Analysis Agent**: Analyzes article content using LLM

The system is coordinated by the **NewsOrchestrator** which manages the workflow between agents through a task manager.
"""
        content += architecture_section
        with open(readme_path, 'w') as f:
            f.write(content)
        logger.info("Added architecture section to README.md")

def main():
    """Run the cleanup script"""
    logger.info("Starting project cleanup...")
    
    # Archive legacy files
    archive_legacy_files()
    
    # Create migration notices
    create_migration_notice()
    
    # Update README
    update_readme()
    
    logger.info("Project cleanup complete. The transition to micro-agent architecture is now finished.")

if __name__ == "__main__":
    main()