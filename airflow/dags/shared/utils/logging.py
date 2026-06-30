# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Logging utilities for the SDG Workflow."""

import logging
import sys


def setup_logger(name: str, logging_level: str = "INFO") -> logging.Logger:
    """
    Setup a logger with consistent formatting for SDG Workflow components.

    Args:
        name: Name of the logger (typically __name__ from the calling module).
        logging_level: Logging level to use (DEBUG, INFO, WARNING, ERROR, CRITICAL).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, logging_level.upper(), logging.INFO))

    # Only add handler if logger doesn't already have handlers
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)

    return logger
