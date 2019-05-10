#!/bin/bash

QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd .. && pwd )"
. ${QPY_SOURCE_DIR}/test/test_functions.sh


#testqpy User1 
#exit

QPY_U1='qpyUser1'
QPY_U1_DIR="${HOME}/.qpy-test_$QPY_U1"

testHeader killing Testing killing jobs

checkIsTestRunning
makeTestDir

echo 'localhost 5' > ${QPY_MU_DIR}/nodes
echo 'even' > ${QPY_MU_DIR}/distribution_rules
echo $QPY_U1 > ${QPY_MU_DIR}/allowed_users

testqpy_multiuser start
sleep 1
print Waiting a cicle in qpy-multiuser...
sleep 15
print

print Creating a users...
for d in ${QPY_U1_DIR}
do
    if [[ -d ${d} ]]
    then
	print Found directory ${d}. Removing it...
	rm -rf ${d}
    fi
    mkdir ${d}
    cp ${QPY_MU_DIR}/multiuser_connection_address ${d}
    cp ${QPY_MU_DIR}/multiuser_connection_conn_key ${d}
    cp ${QPY_MU_DIR}/multiuser_connection_port ${d}
done

testqpy ${QPY_U1} restart
sleep 2

print Users have been created. Waiting a cicle...
sleep 5
testqpy $QPY_U1 config saveMessages true

print Testing killing jobs for user $QPY_U1
print
testqpy $QPY_U1 status
testqpy $QPY_U1 sub -m 0.01 sleep 30
testqpy $QPY_U1 check
print Waiting a bit...
sleep 3
testqpy $QPY_U1 check
sleep 1
testqpy $QPY_U1 kill 1
sleep 1
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
sleep 1
testqpy $QPY_U1 check
print
sleep 5
testqpy $QPY_U1 check
testqpy $QPY_U1 kill 3-5 7,9
print
sleep 3
for i in 1 2 3 4
do
    print Waiting 10 seconds, $i/4
    sleep 10
    testqpy $QPY_U1 check
done

testqpy $QPY_U1 config

showMUlog
showUlog $QPY_U1

finish_test $QPY_U1

