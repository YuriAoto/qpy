from unittest import TestCase, defaultTestLoader, TextTestRunner

import qpy_useful_cosmetics as qpyutil

# To test copy the tests (and imports) into
# qpy-master and execute the following lines
# suite=defaultTestLoader(verbosity=2).LoadTestsFromTestCase(
#      class_that_contains_tests_you want to execute)
# TextTestRunner.run(suite)


class TestGetPlural(TestCase):
    """ class to test the get_plural function"""
    def test_zero(self):
        inp1, inp2 = ("job", "jobs"), 0
        out = ("jobs", "No")
        self.assertEqual(qpyutil.get_plural(inp1, inp2), out)

    def test_one(self):
        inp1, inp2 = ("job", "jobs"), 1
        out = ("job", "1")
        self.assertEqual(qpyutil.get_plural(inp1, inp2), out)
    
    def test_many(self):
        inp1, inp2 = ("job", "jobs"), 16
        out = ("jobs", "16")
        self.assertEqual(qpyutil.get_plural(inp1, inp2), out)
    
    def test_no(self):
        inp1, inp2 = ("job", "jobs"), []
        out = ("jobs", "No")
        self.assertEqual(qpyutil.get_plural(inp1, inp2), out)
    
    def test_single(self):
        inp1, inp2 = ("job", "jobs"), ["running"]
        out = ("job", "running")
        self.assertEqual(qpyutil.get_plural(inp1, inp2), out)
    
    def test_multiple(self):
        inp1, inp2 = ("job", "jobs"), ["queued", "running", "killed"]
        out = ("jobs", "queued, running and killed")
        self.assertEqual(qpyutil.get_plural(inp1, inp2), out)
