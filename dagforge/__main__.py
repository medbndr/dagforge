import argparse
import json
import sqlite3
from pathlib import Path
from .core import execute

def main():
    p = argparse.ArgumentParser(description='Execute or resume a pure JSON workflow')
    p.add_argument('workflow',type=Path)
    p.add_argument('--db',default='runs.db')
    a = p.parse_args()
    try:
        result = execute(json.loads(a.workflow.read_text(encoding='utf-8')),a.db)
        print(json.dumps(result,indent=2))
        if any(s in ('failed','blocked') for s in result['states'].values()):
            raise SystemExit(1)
    except (OSError,ValueError,KeyError,TypeError,sqlite3.Error) as exc:
        p.exit(2,f'error: {exc}\n')

if __name__ == '__main__':
    main()
