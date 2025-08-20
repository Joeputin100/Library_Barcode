#!/usr/bin/env bash
find . -name "*.py" -print0 | xargs -0 black --line-length 79
