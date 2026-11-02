import tempfile
import unittest
from pathlib import Path
from dagforge.core import execute, operation, validate

def literal(i,v):
    return dict(id=i,op='literal',value=v)

class Engine(unittest.TestCase):
    def test_pipeline(self):
        r = execute({'tasks':[literal('a',2),literal('b',3),dict(id='c',op='product',deps=['a','b'])]})
        self.assertEqual(r['outputs']['c'],6)

    def test_resume(self):
        with tempfile.TemporaryDirectory() as d:
            db = str(Path(d)/'runs.db')
            spec = {'tasks':[literal('a',8)]}
            execute(spec,db)
            r = execute(spec,db,lambda *_: self.fail('cached task executed'))
            self.assertEqual(r['states']['a'],'cached')
            self.assertEqual(execute({'tasks':[literal('a',9)]},db)['outputs']['a'],9)

    def test_retry(self):
        calls = []
        def runner(t,inputs):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError('temporary')
            return operation(t,inputs)
        t = literal('a',3)
        t['retries'] = 1
        self.assertEqual(execute({'tasks':[t]},runner=runner)['outputs']['a'],3)
        self.assertEqual(len(calls),2)

    def test_failure_isolation(self):
        r = execute({'tasks':[literal('a','bad'),dict(id='b',op='sum',deps=['a']),
                              dict(id='c',op='sum',deps=['b']),literal('d',10)]})
        self.assertEqual(r['states'],{'a':'success','d':'success','b':'failed','c':'blocked'})

    def test_cycle(self):
        with self.assertRaises(ValueError):
            validate({'tasks':[dict(id='a',op='sum',deps=['b']),dict(id='b',op='sum',deps=['a'])]})

    def test_unknown_dep(self):
        with self.assertRaises(ValueError):
            validate({'tasks':[dict(id='a',op='sum',deps=['missing'])]})

    def test_duplicate_id(self):
        with self.assertRaises(ValueError):
            validate({'tasks':[literal('a',1),literal('a',2)]})

    def test_nonfinite_not_persisted(self):
        r = execute({'tasks':[literal('a',1)]},runner=lambda *_:float('nan'))
        self.assertEqual(r['states']['a'],'failed')
