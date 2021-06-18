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
    echo 'node1 cores=6 address=localhost bigmem' > ${QPY_MU_DIR}/nodes
    echo 'node2 cores=6 address=localhost infiband' >> ${QPY_MU_DIR}/nodes
    echo 'node3 cores=6 address=localhost bigmem infiband' >> ${QPY_MU_DIR}/nodes
    echo 'node4 cores=6 address=localhost' >> ${QPY_MU_DIR}/nodes
    echo 'node5 cores=6 address=localhost' >> ${QPY_MU_DIR}/nodes
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
printBox Running some jobs with attributes in script:
wait_for 3

echo '#QPY node_attr=bigmem and infiband' > `qpyWdir $QPY_U1`/script.sh
echo '#QPY mem=0.1' >> `qpyWdir $QPY_U1`/script.sh
echo 'date' >> `qpyWdir $QPY_U1`/script.sh
echo 'hostname' >> `qpyWdir $QPY_U1`/script.sh
echo 'echo $QPY_NODE' >> `qpyWdir $QPY_U1`/script.sh
echo 'date' >> `qpyWdir $QPY_U1`/script.sh


runUser $QPY_U1 cat script.sh
runUser $QPY_U1 chmod +x script.sh

testqpy $QPY_U1 sub ./script.sh
testqpy $QPY_U1 sub -a infiband -m 0.01 bash script.sh

n=3
for i in `seq $n`
do
    wait_for 3 $i $n
    testqpy $QPY_U1 status
    testqpy $QPY_U1 check
done

runUser $QPY_U1 cat job_1.out
runUser $QPY_U1 cat job_1.err



# =====
# The end
showMUlog
for user in $all_users
do
    testqpy $user config
    showUlog $user
done
finish_test $all_users

