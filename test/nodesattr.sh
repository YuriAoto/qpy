#!/bin/bash
# qpy test

QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd .. && pwd )"
. ${QPY_SOURCE_DIR}/test/test_functions.sh

thisScript=`basename "$0"`

testHeader $thisScript Nodes attributes
checkIsTestRunning
makeTestDir

# =====
# Nodes
echo 'node1 6 address=localhost big' > ${QPY_MU_DIR}/nodes
echo 'node2 6 address=localhost infiband' >> ${QPY_MU_DIR}/nodes
echo 'node3 6 address=localhost big infiband' >> ${QPY_MU_DIR}/nodes
echo 'node4 6 address=localhost' >> ${QPY_MU_DIR}/nodes
echo 'node5 6 address=localhost' >> ${QPY_MU_DIR}/nodes
echo 'even' > ${QPY_MU_DIR}/distribution_rules

# =====
# Users
QPY_U1='qpyUser1'
all_users="$QPY_U1"

# =====
# Go!
testqpy_multiuser start
sleep 1
showMUlog
print Waiting a cicle in qpy-multiuser...
sleep 15
print
testqpy_multiuser status

createUser ${QPY_U1}
testqpy ${QPY_U1} restart
sleep 2

print User has been created. Waiting a cicle...
sleep 5
showUlog $QPY_U1


printBox Changing check pattern to see the attributes of jobs in queue
sleep 3
testqpy $QPY_U1 config checkFMT '%j (%s):%c (on %A; wd: %d)\n'
sleep 3


#=========
printBox Running some jobs without attributes:
sleep 3

testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
for i in 1 2 3 4
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 2
done
for i in 1 2
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 10
done


#=========
printBox Running some jobs with attributes: big:
sleep 3

testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
for i in 1 2 3 4
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 2
done
for i in 1 2
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 10
done

#=========
printBox Running some jobs with attributes: "'(big)and(infiband)'":
sleep 3

testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
for i in 1 2 3 4
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 2
done
for i in 1 2
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 10
done


#=========
printBox Running some jobs with attributes: "'not((big)or(infiband))'":
sleep 3

testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
for i in 1 2 3 4
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 2
done
for i in 1 2
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 10
done


#=========
printBox Adding default attribute, AND type
testqpy $QPY_U1 config andAttr big
sleep 3
testqpy $QPY_U1 config
sleep 3

testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
for i in `seq 1 10`
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 5
done


#=========
printBox Adding default attribute, of OR type
testqpy $QPY_U1 config andAttr 
testqpy $QPY_U1 config orAttr big
sleep 3
testqpy $QPY_U1 config
sleep 3

testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
for i in `seq 1 10`
do
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
    sleep 5
done


# =====
# The end
showMUlog
for user in $all_users
do
    testqpy $user config
    showUlog  $user
done
finish_test $all_users

