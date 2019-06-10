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
if [[ -f nodes ]]
then
    cp nodes ${QPY_MU_DIR}/nodes
else
    echo 'localhost 5' > ${QPY_MU_DIR}/nodes
fi
echo 'even' > ${QPY_MU_DIR}/distribution_rules
echo $QPY_U1 > ${QPY_MU_DIR}/allowed_users

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
print User has been created.
wait_for 10

testqpy $QPY_U1 config
testqpy $QPY_U1 config saveMessages true
testqpy $QPY_U1 config
testqpy $QPY_U1 config maxMessages qwe
testqpy $QPY_U1 config
testqpy $QPY_U1 config maxMessages 10
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
