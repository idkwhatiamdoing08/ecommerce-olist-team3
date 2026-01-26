import sys
print(f"Python {sys.version}")
# Основные библиотеки из requirements.txt
try:
    import pandas as pd
    print(f"✅ pandas {pd.__version__}")
except ImportError as e:
    print(f"❌ pandas: {e}")

try:
    import numpy as np
    print(f"✅ numpy {np.__version__}")
except ImportError as e:
    print(f"❌ numpy: {e}")

try:
    import matplotlib
    print(f"✅ matplotlib {matplotlib.__version__}")
except ImportError as e:
    print(f"❌ matplotlib: {e}")

# SQLite3 (встроенный)
try:
    import sqlite3
    print(f"✅ sqlite3 {sqlite3.sqlite_version}")
except ImportError as e:
    print(f"❌ sqlite3: {e}")

# Проверим пути импортов проекта
print("\n🔍 Проверяем структуру проекта...")
