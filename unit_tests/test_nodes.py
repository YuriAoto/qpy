"""Tests for nodes

"""
import os
import unittest
import logging

import unit_tests
import qpy_nodes_management
import qpy_system

import qpy_logging


working_nodes_file = 'real_nodes_file'

class Printing(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        self.n1 = qpy_nodes_management.Node('node1')
        self.n1.is_up = True
        self.n1.max_cores = 10
        self.n1.n_used_cores = 2
        self.n1.total_mem = 50.0
        self.n1.used_mem = 30.0
        self.n1.load = 1.0
        self.n1.total_disk = 100.0
        self.n1.used_disk = 80.0

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        reference = ('node1                            '
                     '  2    10     1.0     30.0     2.0    50.0    80    100')
        x = str(self.n1)
        self.assertEqual(x, reference)

    def test2(self):
        reference = ('node1 (down)                     '
                     '  2    10     1.0     30.0     2.0    50.0    80    100')
        self.n1.is_up = False
        x = str(self.n1)
        self.assertEqual(x, reference)


class ConstructNode(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        n1 = qpy_nodes_management.Node.from_string('node1 cores=10')
        self.assertEqual(n1.name, 'node1')
        self.assertEqual(n1.max_cores, 10)

    def test2(self):
        n1 = qpy_nodes_management.Node.from_string('node1 cores=20 attributes=big,fast')
        self.assertEqual(n1.name, 'node1')
        self.assertEqual(n1.max_cores, 20)
        self.assertEqual(n1.attributes[0], 'big')
        self.assertEqual(n1.attributes[1], 'fast')

    def test3(self):
        n1 = qpy_nodes_management.Node.from_string('node1 memory=100')
        self.assertEqual(n1.name, 'node1')
        self.assertEqual(n1.total_mem, 100.0)


class NodeResource(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        self.n1 = qpy_nodes_management.Node.from_string('node1 cores=10')

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        self.n1.allocate_resource(1, 1.5)
        self.assertEqual(self.n1.n_used_cores, 1)
        self.assertAlmostEqual(self.n1.req_mem, 3.5)
        self.n1.allocate_resource(3, 4.0)
        self.assertEqual(self.n1.n_used_cores, 4)
        self.assertAlmostEqual(self.n1.req_mem, 7.5)
        self.n1.free_resource(1, 1.5)
        self.assertEqual(self.n1.n_used_cores, 3)
        self.assertAlmostEqual(self.n1.req_mem, 6.0)


class ConstructNodes(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        self.fname = 'tmp_nodes_file_testing'
        with open(self.fname, 'w') as f:
            f.write('node1 cores=10\n')
            f.write('node2 cores=20\n')
            f.write('node3 cores=30\n')

    def tearDown(self):
        os.remove(self.fname)
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        nodes = qpy_nodes_management.NodesCollection()
        nodes.load_from_file(self.fname)
        self.assertEqual(len(nodes), 3)
        self.assertEqual(nodes['node1'].name, 'node1')
        self.assertEqual(nodes['node1'].max_cores, 10)
        self.assertEqual(nodes['node2'].name, 'node2')
        self.assertEqual(nodes['node2'].max_cores, 20)
        self.assertEqual(nodes['node3'].name, 'node3')
        self.assertEqual(nodes['node3'].max_cores, 30)


class PrintingNodes(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        n1 = qpy_nodes_management.Node('node1')
        n1.is_up = True
        n1.max_cores = 10
        n1.n_used_cores = 2
        n1.total_mem = 50.0
        n1.used_mem = 30.0
        n1.load = 1.0
        n1.total_disk = 100.0
        n1.used_disk = 80.0
        n2 = qpy_nodes_management.Node('node2')
        n2.is_up = False
        n2.max_cores = 100
        n2.n_used_cores = 20
        n2.req_mem = 20.0
        n2.total_mem = 100.0
        n2.used_mem = 80.0
        n2.load = 3.0
        n2.total_disk = 200.0
        n2.used_disk = 170.0

        self.nodes = qpy_nodes_management.NodesCollection()
        self.nodes.add_node(n1)
        self.nodes.add_node(n2)

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        reference = (
'                                   cores                    memory (GB)       disk (GB)\n'
'node                            used  total   load     used     req   total  used  total\n'
'----------------------------------------------------------------------------------------\n'
'node1                              2    10     1.0     30.0     2.0    50.0    80    100\n'
'node2 (down)                      20   100     3.0     80.0    20.0   100.0   170    200\n'
'========================================================================================\n'
'There are 0 out of a total of 110 cores being used.'
        )
        x = str(self.nodes)
        self.assertEqual(x, reference)


class NodesAddRemoveNode(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        n1 = qpy_nodes_management.Node('node1')
        n2 = qpy_nodes_management.Node('node2')
        nodes = qpy_nodes_management.NodesCollection()
        with nodes.check_lock:
            nodes.add_node(n1)
            nodes.add_node(n2)
        self.assertEqual(nodes._the_names[0], 'node1')
        self.assertEqual(nodes._the_names[1], 'node2')
        self.assertEqual(nodes._the_nodes[0].name, 'node1')
        self.assertEqual(nodes._the_nodes[1].name, 'node2')
        nodes.remove_node('node1')
        self.assertEqual(nodes._the_names[0], 'node2')
        self.assertEqual(nodes._the_nodes[0].name, 'node2')


class NodesDataModel(unittest.TestCase):
    
    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        self.nodes = qpy_nodes_management.NodesCollection()
        self.nodes.add_node(qpy_nodes_management.Node('node1'))
        self.nodes.add_node(qpy_nodes_management.Node('node2'))

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test_len(self):
        self.assertEqual(len(self.nodes), 2)

    def test_getitem(self):
        n = self.nodes['node1']
        self.assertEqual(n.name, 'node1')
        n = self.nodes['node2']
        self.assertEqual(n.name, 'node2')
        with self.assertRaises(KeyError):
            self.nodes['node3']

    def test_contains(self):
        self.assertTrue('node1' in self.nodes)
        self.assertTrue('node2' in self.nodes)
        self.assertFalse('node3' in self.nodes)

    def test_iter(self):
        for i, n in enumerate(self.nodes):
            if i == 0:
                self.assertEqual(n.name, 'node1')
            else:
                self.assertEqual(n.name, 'node2')

    def test_iter_items(self):
        for i, nn in enumerate(self.nodes.items()):
            name, n = nn
            if i == 0:
                self.assertEqual(n.name, 'node1')
                self.assertEqual(name, 'node1')
            else:
                self.assertEqual(n.name, 'node2')
                self.assertEqual(name, 'node2')


class Checking(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        self.nodes = qpy_nodes_management.NodesCollection()
        self.nodes.load_from_file(working_nodes_file)
        for n in self.nodes:
            n.check_dir = '/scr/'

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test_1(self):
        self.nodes.check()
        print(self.nodes)

