#!/bin/bash

QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd .. && pwd )"
. ${QPY_SOURCE_DIR}/test/test_functions.sh

thisScript=`basename "$0"`

testHeader $thisScript Basic behaviour of nodes
checkIsTestRunning
makeTestDir

# =====
# Nodes
if [[ -f nodes ]]
then
    cp nodes ${QPY_MU_DIR}/nodes
else
    echo 'node1 6 address=localhost' > ${QPY_MU_DIR}/nodes
    echo 'node2 6 address=localhost' >> ${QPY_MU_DIR}/nodes
fi
echo 'even' > ${QPY_MU_DIR}/distribution_rules

# =====
# Users
QPY_U1='qpyUser1'
all_users="$QPY_U1"

# =====
# Go!
testqpy_multiuser start
wait_for 10

createUser ${QPY_U1}
testqpy ${QPY_U1} restart
wait_for 2

print Users have been created.
wait_for 5

testqpy_multiuser status
testqpy_multiuser variables

print Testing basic qpy commands for user $QPY_U1
print
testqpy $QPY_U1 config saveMessages true
testqpy $QPY_U1 status

testqpy $QPY_U1 sub -m 0.01 hostname
testqpy $QPY_U1 check
wait_for 3
testqpy $QPY_U1 check
runUser $QPY_U1 cat job_1.out
runUser $QPY_U1 cat job_1.err

testqpy $QPY_U1 sub -m 0.01 sleep 20
testqpy $QPY_U1 sub -m 0.01 sleep 20
testqpy $QPY_U1 sub -m 0.01 sleep 20
testqpy $QPY_U1 sub -m 0.01 sleep 20
testqpy $QPY_U1 sub -m 0.01 sleep 20
testqpy $QPY_U1 check
wait_for 3
testqpy $QPY_U1 check
testqpy $QPY_U1 status
wait_for 20
testqpy $QPY_U1 check
testqpy $QPY_U1 status


# =====
# The end
#showMUlog
for user in $all_users
do
    testqpy $user config
#    showUlog  $user
done
finish_test $all_users

