#!/bin/bash

QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd .. && pwd )"
. ${QPY_SOURCE_DIR}/test/test_functions.sh

thisScript=`basename "$0"`

testHeader $thisScript Testing killing jobs
checkIsTestRunning
makeTestDir

# =====
# Nodes
if [[ -f nodes ]]
then
    cp nodes ${QPY_MU_DIR}/nodes
else
    echo 'localhost cores=10' > ${QPY_MU_DIR}/nodes
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
print

createUser ${QPY_U1}
testqpy ${QPY_U1} restart
wait_for 2

print User has been created.
wait_for 5

testqpy $QPY_U1 config saveMessages true

print Testing killing jobs for user $QPY_U1
print
testqpy $QPY_U1 status
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 check
wait_for 3
testqpy $QPY_U1 check
wait_for 1
testqpy $QPY_U1 kill 1
wait_for 1
testqpy $QPY_U1 check
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 sub -m 0.01 sleep 30
print
wait_for 1
testqpy $QPY_U1 check
print
wait_for 1
testqpy $QPY_U1 check
testqpy $QPY_U1 kill 3-5 7,9
print
wait_for 3

n=4
for i in `seq $n`
do
    wait_for 10 $i $n
    testqpy $QPY_U1 check
done

testqpy $QPY_U1 config

# =====
# The end
#showMUlog
for user in $all_users
do
    testqpy $user config
#    showUlog  $user
done
finish_test $all_users

