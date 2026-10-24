"""Bound third-party scraper calls in a subprocess; never share DB credentials."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import pandas as pd


def fetch_jobspy_shard(**kwargs):
    with tempfile.TemporaryDirectory(prefix='jobspy-') as directory:
        output=Path(directory)/'jobs.json'
        code='''import json,sys\nfrom jobspy import scrape_jobs\nf=scrape_jobs(**json.loads(sys.argv[1]))\nf.to_json(sys.argv[2],orient="records",date_format="iso")\n'''
        env={k:v for k,v in os.environ.items() if k not in ('DATABASE_URL','DATABASE_URL_UNPOOLED','TELEGRAM_TOKEN','TELEGRAM_CHAT_ID')}
        process=subprocess.run([sys.executable,'-c',code,json.dumps(kwargs),str(output)],
                               capture_output=True,text=True,env=env,timeout=120)
        if process.stderr:
            # JobSpy reports some partial failures only through its logger.
            for line in process.stderr.splitlines():
                if any(word in line.upper() for word in ('ERROR','WARNING')):
                    print('JobSpy subprocess:',line[:400],file=sys.stderr)
        if process.returncode:
            raise RuntimeError('JobSpy shard failed: '+process.stderr[-600:])
        return pd.DataFrame(json.loads(output.read_text()))
