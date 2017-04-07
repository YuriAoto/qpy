import StringIO
import unittest as ut
class StringIOTestWrapper(StringIO.StringIO):
    def __init__(self,*args):
        StringIO.StringIO.__init__(self, *args)
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self,*args):
        self._closed=True

    def close(self):
        self._closed=True

class StringIOTestFactory(object):
            
    def __init__(self):
        self.files = dict()
        self.modes = dict()

    def __call__(self, name, mode):
        if mode == "r":
            self.modes[name] = mode
            return self.files[name]
        else:
            myfile = StringIOTestWrapper()
            self.files[name]=myfile
            self.modes[name]=mode
            return myfile

    def set_file(self, name, content):
        self.files[name]=StringIOTestWrapper(content)
        
class TestCollector():
    def __init__(self):
        self.suites=[]

    def collect(self, cls):
        self.suites.append(ut.TestLoader().loadTestsFromTestCase(cls))

    def run(self, *args,**kwargs):
        return ut.TextTestRunner(*args,**kwargs).run(ut.TestSuite(self.suites))
