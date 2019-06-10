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
if [[ -f nodes ]]
then
    cp nodes ${QPY_MU_DIR}/nodes
else
    echo 'node1 6 address=localhost big' > ${QPY_MU_DIR}/nodes
    echo 'node2 6 address=localhost infiband' >> ${QPY_MU_DIR}/nodes
    echo 'node3 6 address=localhost big infiband' >> ${QPY_MU_DIR}/nodes
    echo 'node4 6 address=localhost' >> ${QPY_MU_DIR}/nodes
    echo 'node5 6 address=localhost' >> ${QPY_MU_DIR}/nodes
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
testqpy $QPY_U1 config saveMessages true

print User has been created.
wait_for 15
testqpy_multiuser status


printBox Changing check pattern to see the attributes of jobs in queue
wait_for 3
testqpy $QPY_U1 config checkFMT '%j (%s):%c (on %A; wd: %d)\n'
wait_for 3


#=========
printBox Running some jobs without attributes:
wait_for 3

testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10

n=4
for i in `seq $n`
do
    wait_for 2 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done
n=2
for i in `seq $n`
do
    wait_for 10 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done


#=========
printBox Running some jobs with attributes: big:
wait_for 3

testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10
testqpy $QPY_U1 sub -m 0.01 -a big sleep 10

n=4
for i in `seq $n`
do
    wait_for 2 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done
n=2
for i in `seq $n`
do
    wait_for 10 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done

#=========
printBox Running some jobs with attributes: "'(big)and(infiband)'":
wait_for 3

testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a '(big)and(infiband)' sleep 10

n=4
for i in `seq $n`
do
    wait_for 2 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done
n=2
for i in `seq $n`
do
    wait_for 10 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done


#=========
printBox Running some jobs with attributes: "'not((big)or(infiband))'":
wait_for 3

testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'not((big)or(infiband))' sleep 10

n=4
for i in `seq $n`
do
    wait_for 2 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done
n=2
for i in `seq $n`
do
    wait_for 10 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done


#=========
printBox Adding default attribute, AND type
testqpy $QPY_U1 config andAttr big
wait_for 3
testqpy $QPY_U1 config
wait_for 3

testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10

n=4
for i in `seq $n`
do
    wait_for 2 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done
n=2
for i in `seq $n`
do
    wait_for 10 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done


#=========
printBox Adding default attribute, of OR type
testqpy $QPY_U1 config andAttr 
testqpy $QPY_U1 config orAttr big
wait_for 3
testqpy $QPY_U1 config
wait_for 3

testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10
testqpy $QPY_U1 sub -m 0.01 -a 'infiband' sleep 10

n=4
for i in `seq $n`
do
    wait_for 2 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done
n=2
for i in `seq $n`
do
    wait_for 10 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done


# =====
# The end
#showMUlog
for user in $all_users
do
    testqpy $user config
#    showUlog $user
done
finish_test $all_users

