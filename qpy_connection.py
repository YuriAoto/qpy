
from multiprocessing.connection import Client
import unittest as ut
import test_util
import os
import threading







class ConnectionConfig(object):
    def __init__(self):
        self.qpy_source_dir = os.path.dirname( os.path.abspath( __file__)) + '/'
        self.home_dir = os.environ['HOME']
        self.user = os.environ['USER']
        self.qpy_dir = os.path.expanduser( '~/.qpy/')
        self.port_file = self.qpy_dir + 'port'
        self.key_file = self.qpy_dir + 'conn_key'
        self.address = 'localhost'
    def get_address(self):
        return self.address


class FileAccessor(object):
    def __init__(self, config, filecls=file):
        self._filecls = filecls
        self._config = config
    def get_port(self):
        with self._filecls(self._config.port_file,"r") as f:
            return int( f.read())
    def get_key(self):
        with self._filecls(self._config.key_file,"r") as f:
            return f.read()
            

class ConnectionThread(threading.Thread):
    """ Threaded connector to the server"""
    def __init__(self, clientfactory):
        """ Takes a clientfactory, which will be called without arguments to create a client"""
        threading.Thread.__init__(self)
        self.conn = None
        self.done = threading.Event()
        self.clientfactory =clientfactory

    def run(self):
        """ call start if you want to run it."""
        try:
            self.conn = self.clientfactory()
            self.done.set()
        except:
            self.done.clear()

class Sender(object):
    """ encapsulates the commutincation protokoll to the server.

    usage:
    >> with Sender(...) as sender:
    >>      ret_msg = sender.send(msg)
    """
    
    @classmethod
    def  from_config(cls,config, fileaccessor, connectionthread_factory= ConnectionThread, client_factory=Client):
        return cls(config.get_address(),
                       fileaccessor.get_port(),
                       fileaccessor.get_key(),
                       connectionthread_factory,
                       client_factory )

    def __init__( self, address, port, conn_key, connectionthread_factory= ConnectionThread, client_factory=Client):
        self._thread = connectionthread_factory(lambda:client_factory( ( address, port), authkey = conn_key))
        self._thread.daemon = True

    def __enter__(self):
            self._thread.start()
            self._thread.done.wait(3.0)
            if ( not (self._thread.done.is_set())):
                    raise SendingError("Connection timed out. Are you sure that qpy-master is running?")
            return self
            
    def send (self , msg):
        self._thread.conn.send(msg)
        return self._thread.conn.recv()

    def __exit__(self, *args):
        self._thread.conn.close()



collector = test_util.TestCollector()

@collector.collect
class TestSender(ut.TestCase):
    class MockConnectionThread(object):
        class MockEvent(object):
            def __init__(self):
                self.fl = False
                self._err = False

            def wait(self, timeout):
                self.fl = not self._err

            def is_set(self):
                return self.fl

            def set(self):
                self.fl=True

            def clear(self):
                self.fl =False
                
        def __init__(self,clientfactory):
            self.conn = None
            self.done = self.MockEvent()
            self.factory = clientfactory
            self.daemon = False
            self._err = False
            
        def start(self):
            self.conn = self.factory()
            self.done._err = self._err

    class MockClient(object):
        master_msg = "This is your master"
        def __init__(self, *args,**kwargs):
            pass
        
        def send(self, msg):
            self._msg = msg
            
        def recv(self):
            return self.master_msg
        def close(self):
            self._is_closed = True
        
        
        
    def test_successfull(self):
        with Sender("localhost", 1123, "asdf", self.MockConnectionThread, self.MockClient) as sender:
            self.assertEqual(sender.send((2,"go there")), "This is your master")
        self.assertTrue(sender._thread.daemon )
        self.assertTrue(sender._thread.conn._is_closed)
            
    def test_from_config(self):
        factory=test_util.StringIOTestFactory()
        port_file = test_util.StringIOTestWrapper()
        port_file.write("1000")
        port_file.seek(0)
        factory.files[os.path.expanduser( '~/.qpy/port')]=port_file
        port_file = test_util.StringIOTestWrapper()
        port_file.write("asdf")
        port_file.seek(0)
        factory.files[os.path.expanduser( '~/.qpy/conn_key')]=port_file
        conf=MainConfig()
        with Sender.from_config(conf,FileAccessor(conf,factory),
                                    self.MockConnectionThread,
                                    self.MockClient) as sender:
            self.assertEqual(sender.send((2,"go there")), "This is your master")
            pass

if __name__ == "__main__":
        collector.run()


