#!/usr/bin/env python3
"""Setup and validation script for CLI environment (Codex, Claude Code, Kimi).

This script helps configure and validate your environment for using
different CLI backends with the workflow runner.

Usage:
    python setup_cli_env.py --check           # Check current setup
    python setup_cli_env.py --setup-kimi      # Show Kimi setup instructions
    python setup_cli_env.py --setup-claude    # Show Claude + Kimi setup
    python setup_cli_env.py --setup-codex     # Show Codex setup
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Add script directory to path for imports
_scripts_dir = Path(__file__).parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
if str(_scripts_dir.parent) not in sys.path:
    sys.path.insert(0, str(_scripts_dir.parent))

try:
    from unified_cli_runner import validate_claude_kimi_setup, detect_available_cli
except ImportError:
    from scripts.unified_cli_runner import validate_claude_kimi_setup, detect_available_cli


def check_cli_installed(name: str) -> tuple[bool, str]:
    """Check if a CLI tool is installed and get its version."""
    path = shutil.which(name)
    if not path:
        return False, "Not found in PATH"
    
    try:
        result = subprocess.run(
            [name, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        version = result.stdout.strip() or result.stderr.strip()
        return True, f"{path} ({version[:50]})"
    except Exception as e:
        return True, f"{path} (version check failed: {e})"


def check_environment() -> dict[str, Any]:
    """Check all environment variables and CLI availability."""
    results = {
        "cli_tools": {},
        "env_vars": {},
        "recommendations": [],
    }
    
    # Check CLI tools
    for cli in ["codex", "claude", "kimi"]:
        installed, info = check_cli_installed(cli)
        results["cli_tools"][cli] = {"installed": installed, "info": info}
    
    # Check environment variables
    env_vars = [
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_MODEL",
        "OPENAI_API_KEY",
    ]
    
    for var in env_vars:
        value = os.environ.get(var, "")
        # Mask sensitive values
        if value and ("KEY" in var or "TOKEN" in var):
            display = value[:10] + "..." + value[-4:] if len(value) > 14 else "***"
        else:
            display = value
        results["env_vars"][var] = display
    
    # Detect configuration
    base_url = os.environ.get("ANTHROPIC_BASE_URL", "")
    
    if "moonshot" in base_url.lower() or "kimi" in base_url.lower():
        results["detected_backend"] = "kimi"
        is_valid, msg = validate_claude_kimi_setup()
        results["claude_kimi_valid"] = is_valid
        results["claude_kimi_msg"] = msg
        
        if not is_valid:
            results["recommendations"].append(f"Claude + Kimi configuration issue: {msg}")
    elif "anthropic" in base_url.lower():
        results["detected_backend"] = "anthropic"
    elif base_url:
        results["detected_backend"] = "custom"
    else:
        results["detected_backend"] = "not_set"
    
    # Generate recommendations
    available_clis = [cli for cli, info in results["cli_tools"].items() if info["installed"]]
    
    if not available_clis:
        results["recommendations"].append(
            "No CLI tools found! Install at least one: codex, claude, or kimi"
        )
    else:
        results["recommendations"].append(
            f"Available CLIs: {', '.join(available_clis)}. "
            f"Use: --cli {available_clis[0]}"
        )
    
    if results["detected_backend"] == "kimi":
        if not results["cli_tools"]["claude"]["installed"]:
            results["recommendations"].append(
                "For best results with Kimi, install Claude Code CLI: npm install -g @anthropic-ai/claude-code"
            )
        if results.get("claude_kimi_valid"):
            results["recommendations"].append(
                "✓ Claude + Kimi configuration looks good! Use: --cli claude"
            )
    
    if results["detected_backend"] == "not_set":
        if results["cli_tools"]["codex"]["installed"]:
            results["recommendations"].append(
                "No ANTHROPIC_BASE_URL set. Using Codex (OpenAI) backend."
            )
        elif results["cli_tools"]["kimi"]["installed"]:
            results["recommendations"].append(
                "Consider setting up ANTHROPIC_BASE_URL to use Claude Code with Kimi "
                "for better structured output."
            )
    
    return results


def print_check_results(results: dict):
    """Print check results in a formatted way."""
    print("\n" + "=" * 60)
    print("CLI Environment Check")
    print("=" * 60)
    
    print("\n📦 CLI Tools:")
    for cli, info in results["cli_tools"].items():
        status = "✓" if info["installed"] else "✗"
        print(f"  {status} {cli}: {info['info']}")
    
    print("\n🔧 Environment Variables:")
    for var, value in results["env_vars"].items():
        if value:
            print(f"  ✓ {var}={value}")
        else:
            print(f"  ○ {var}=(not set)")
    
    print(f"\n🎯 Detected Backend: {results.get('detected_backend', 'unknown')}")
    
    if "claude_kimi_valid" in results:
        status = "✓" if results["claude_kimi_valid"] else "✗"
        print(f"  {status} Claude+Kimi: {results['claude_kimi_msg']}")
    
    print("\n💡 Recommendations:")
    for rec in results["recommendations"]:
        print(f"  • {rec}")
    
    print("\n" + "=" * 60)


def print_kimi_setup():
    """Print Kimi setup instructions."""
    print("""
🌙 Kimi CLI Setup
================

Kimi CLI is the native Moonshot CLI tool.

Installation:
    pip install kimi-cli
    # or
    uv tool install kimi-cli

Configuration:
    # Get API key from https://platform.moonshot.cn/console/api-keys
    kimi login

Usage with workflow:
    python scripts/run_project.py --project <name> --workflow <name> --cli kimi

Note: Kimi CLI doesn't have native structured output, so we use a retry
wrapper. For better reliability, consider using Claude Code with Kimi backend.
""")


def print_claude_setup():
    """Print Claude Code + Kimi setup instructions."""
    print("""
🤖 Claude Code + Kimi Setup
===========================

This gives you Claude Code's excellent structured output with Kimi's models.

Installation:
    npm install -g @anthropic-ai/claude-code

Configuration (choose one):

1. Standard Moonshot API (kimi-k2.5, etc.):
    export ANTHROPIC_BASE_URL="https://api.moonshot.cn/anthropic"
    export ANTHROPIC_API_KEY="sk-your-moonshot-key"
    export ANTHROPIC_MODEL="kimi-k2.5"

2. Kimi for Coding (subscription):
    export ANTHROPIC_BASE_URL="https://api.kimi.com/coding/"
    export ANTHROPIC_API_KEY="your-kimi-for-coding-key"
    export ANTHROPIC_MODEL="kimi-for-coding"

3. Global/Moonshot AI endpoint:
    export ANTHROPIC_BASE_URL="https://api.moonshot.ai/anthropic"
    export ANTHROPIC_API_KEY="sk-your-global-key"

Add to ~/.zshrc or ~/.bashrc for persistence.

Test your setup:
    claude -p "Hello" --output-format json

Usage with workflow:
    python scripts/run_project.py --project <name> --workflow <name> --cli claude

Available models:
    - kimi-k2.5 (recommended, 256K context)
    - kimi-k2-turbo-preview (faster, 60-100 tokens/s)
    - kimi-k2-thinking (extended thinking mode)
    - kimi-k2-thinking-turbo (thinking + speed)
    - kimi-for-coding (subscription, requires coding endpoint)

Note: Claude Code uses --json-schema for structured output, which is
validated by the model. This is more reliable than Kimi's native output.
""")


def print_codex_setup():
    """Print Codex setup instructions."""
    print("""
🛠️  OpenAI Codex Setup
=====================

Codex is OpenAI's coding agent CLI with built-in structured output.

Installation:
    npm install -g @openai/codex

Configuration:
    export OPENAI_API_KEY="sk-your-openai-key"

Usage with workflow:
    python scripts/run_project.py --project <name> --workflow <name> --cli codex

Note: Codex uses OpenAI's models (GPT-4o, etc.). For Kimi models,
use Claude Code with Kimi backend instead (--cli claude).
""")


def main():
    parser = argparse.ArgumentParser(description="Setup and validate CLI environment")
    parser.add_argument("--check", action="store_true", help="Check current environment")
    parser.add_argument("--setup-kimi", action="store_true", help="Show Kimi CLI setup")
    parser.add_argument("--setup-claude", action="store_true", help="Show Claude + Kimi setup")
    parser.add_argument("--setup-codex", action="store_true", help="Show Codex setup")
    parser.add_argument("--setup-all", action="store_true", help="Show all setup instructions")
    args = parser.parse_args()
    
    if args.setup_kimi:
        print_kimi_setup()
        return
    
    if args.setup_claude:
        print_claude_setup()
        return
    
    if args.setup_codex:
        print_codex_setup()
        return
    
    if args.setup_all:
        print_kimi_setup()
        print_claude_setup()
        print_codex_setup()
        return
    
    # Default: check environment
    results = check_environment()
    print_check_results(results)
    
    # Exit with error if no CLIs found
    available = [cli for cli, info in results["cli_tools"].items() if info["installed"]]
    if not available:
        print("\n❌ No CLI tools found. Install one to proceed.")
        sys.exit(1)
    else:
        print(f"\n✓ Ready to use: {', '.join(available)}")


if __name__ == "__main__":
    main()
