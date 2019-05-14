#!/bin/bash
# Test for qpy

QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd .. && pwd )"
. ${QPY_SOURCE_DIR}/test/test_functions.sh

thisScript=`basename "$0"`

testHeader $thisScript Configurations and their changes
checkIsTestRunning
makeTestDir

# =====
# Nodes
echo 'localhost 5' > ${QPY_MU_DIR}/nodes
echo 'even' > ${QPY_MU_DIR}/distribution_rules
echo $QPY_U1 > ${QPY_MU_DIR}/allowed_users

# =====
# Users
QPY_U1='qpyUser1'
all_users="$QPY_U1"

# =====
# Go!
testqpy_multiuser start
sleep 1
print Waiting a cicle in qpy-multiuser...
sleep 15
print

createUser ${QPY_U1}
testqpy ${QPY_U1} restart
print User has been created. Waiting a cicle...
sleep 5

testqpy $QPY_U1 config
testqpy $QPY_U1 config saveMessages true
testqpy $QPY_U1 config
testqpy $QPY_U1 config maxMessages qwe
testqpy $QPY_U1 config
testqpy $QPY_U1 config maxMessages 10
testqpy $QPY_U1 config

showUlog $QPY_U1
testqpy $QPY_U1 config


# =====
# The end
showMUlog
for user in $all_users
do
    testqpy $user config
    showUlog  $user
done
finish_test $all_users
