#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Punto de entrada heredado (Api/main.py).
Redirige la ejecución al nuevo orquestador principal del proyecto (main.py).
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from main import run_pipeline

if __name__ == "__main__":
    run_pipeline()
