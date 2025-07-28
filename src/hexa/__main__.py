#!/usr/bin/env python3
"""
Entry point for the hexa CLI application.
This allows the package to be run with: python -m hexa
"""

from .cli import app

if __name__ == "__main__":
    app()