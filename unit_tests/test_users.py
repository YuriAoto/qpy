"""Tests for nodes

"""
import os
import unittest

import unit_tests
import qpy_users_management
import qpy_system

class Printing(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        self.u1 = qpy_users_management.User('user1', None, None, None)
        self.u1.n_used_cores = 10
        self.u1.n_queue = 8

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        reference = 'user1                            10                8    '
        x = str(self.u1)
        self.assertEqual(x, reference)

    def test2(self):
        self.u1.n_queue = 0
        reference = 'user1                            10                0    '
        x = str(self.u1)
        self.assertEqual(x, reference)


class PrintingNodes(unittest.TestCase):

    def setUp(self):
        os.system(f'touch {qpy_system.source_dir}/test_dir')
        u1 = qpy_users_management.User('user1', None, None, None)
        u1.n_used_cores = 10
        u1.n_queue = 8
        u2 = qpy_users_management.User('user2', None, None, None)
        u2.n_used_cores = 4
        u2.n_queue = 0
        self.users = qpy_users_management.UsersCollection()
        self.users._the_users['user1'] = u1
        self.users._the_users['user2'] = u2

    def tearDown(self):
        os.remove(f'{qpy_system.source_dir}/test_dir')

    def test1(self):
        reference = (
"""user                          using cores        queue size
----------------------------------------------------------------------------------------
user1                            10                8    
user2                            4                 0    
========================================================================================"""
        )
        x = str(self.users)
        self.assertEqual(x, reference)


