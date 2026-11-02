"""A small durable scheduler for pure DAG operations."""
import hashlib
import json
import math
import sqlite3
from collections import deque

def validate(spec):
    tasks = spec['tasks']
    ids = [t['id'] for t in tasks]
    if any(not isinstance(i,str) or not i for i in ids) or len(set(ids)) != len(ids):
        raise ValueError('task IDs must be unique nonempty strings')
    by_id = {t['id']:t for t in tasks}
    children = {i:[] for i in ids}
    indegree = {}
    for t in tasks:
        deps = t.get('deps',[])
        if len(set(deps)) != len(deps) or any(d not in by_id for d in deps):
            raise ValueError('dependencies must be distinct existing task IDs')
        if t['op'] not in {'literal','sum','product','concat'}:
            raise ValueError('unsupported operation')
        retries = t.get('retries',0)
        if type(retries) is not int or not 0 <= retries <= 10:
            raise ValueError('retries must be an integer from 0 to 10')
        if t['op'] == 'literal' and deps:
            raise ValueError('literal tasks cannot depend on other tasks')
        if t['op'] == 'literal' and 'value' not in t:
            raise ValueError('literal task needs a value')
        indegree[t['id']] = len(deps)
        for dep in deps:
            children[dep].append(t['id'])
    q = deque(i for i in ids if indegree[i] == 0)
    order = []
    while q:
        i = q.popleft()
        order.append(i)
        for c in children[i]:
            indegree[c] -= 1
            if indegree[c] == 0:
                q.append(c)
    if len(order) != len(tasks):
        raise ValueError('workflow contains a dependency cycle')
    return by_id, order

def operation(task, inputs):
    op = task['op']
    if op == 'literal':
        return task['value']
    if op == 'concat':
        return task.get('separator','').join(map(str,inputs))
    if any(type(x) not in (int,float) or not math.isfinite(x) for x in inputs):
        raise ValueError('numeric operations require finite numbers')
    return sum(inputs) if op == 'sum' else math.prod(inputs)

def execute(spec, database=':memory:', runner=operation):
    tasks, order = validate(spec)
    encoded = json.dumps(spec,sort_keys=True,allow_nan=False).encode()
    run_id = hashlib.sha256(encoded).hexdigest()
    con = sqlite3.connect(database)
    try:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS results (
          run_id TEXT, task_id TEXT, output TEXT NOT NULL,
          PRIMARY KEY(run_id, task_id));
        CREATE TABLE IF NOT EXISTS attempts (
          run_id TEXT, task_id TEXT, status TEXT, detail TEXT,
          recorded_at TEXT DEFAULT CURRENT_TIMESTAMP);
        """)
        outputs = {i:json.loads(value) for i,value in con.execute(
            'SELECT task_id,output FROM results WHERE run_id=?',(run_id,))}
        states = {}
        for i in order:
            task = tasks[i]
            if i in outputs:
                states[i] = 'cached'
                continue
            if any(dep not in outputs for dep in task.get('deps',[])):
                states[i] = 'blocked'
                continue
            for _ in range(task.get('retries',0)+1):
                try:
                    output = runner(task,[outputs[d] for d in task.get('deps',[])])
                    serialized = json.dumps(output,allow_nan=False)
                except Exception as exc:
                    with con:
                        con.execute('INSERT INTO attempts(run_id,task_id,status,detail) VALUES(?,?,?,?)',
                                    (run_id,i,'failed',str(exc)))
                    states[i] = 'failed'
                else:
                    with con:
                        con.execute('INSERT OR REPLACE INTO results VALUES(?,?,?)',(run_id,i,serialized))
                        con.execute('INSERT INTO attempts(run_id,task_id,status,detail) VALUES(?,?,?,?)',
                                    (run_id,i,'success',''))
                    outputs[i], states[i] = output, 'success'
                    break
        return {'run_id':run_id,'states':states,'outputs':outputs}
    finally:
        con.close()
