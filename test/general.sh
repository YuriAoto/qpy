#!/bin/bash

QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd .. && pwd )"
. ${QPY_SOURCE_DIR}/test/test_functions.sh


QPY_U1='qpyUser1'
QPY_U2='qpyUser2'
QPY_U3='qpyUser3'
QPY_U1_DIR="${HOME}/.qpy-test_$QPY_U1"
QPY_U2_DIR="${HOME}/.qpy-test_$QPY_U2"
QPY_U3_DIR="${HOME}/.qpy-test_$QPY_U3"

testHeader general General test, only localhost and three users, with the basic commands

checkIsTestRunning
makeTestDir

echo 'localhost 5' > ${QPY_MU_DIR}/nodes
echo 'even' > ${QPY_MU_DIR}/distribution_rules
echo $QPY_U1 > ${QPY_MU_DIR}/allowed_users
echo $QPY_U2 >> ${QPY_MU_DIR}/allowed_users
echo $QPY_U3 >> ${QPY_MU_DIR}/allowed_users

testqpy_multiuser start
print Waiting a cicle in qpy-multiuser...
sleep 15
print
testqpy_multiuser status
print

print Creating three users...
for d in ${QPY_U1_DIR} ${QPY_U2_DIR} ${QPY_U3_DIR}
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
testqpy ${QPY_U2} restart
sleep 2
testqpy ${QPY_U3} restart
sleep 2

print Users have been created. Waiting a cicle...
sleep 5

testqpy_multiuser status
testqpy_multiuser variables

print Testing basic qpy commands for user $QPY_U1
print
testqpy $QPY_U1 config saveMessages true
testqpy $QPY_U1 status

testqpy $QPY_U1 sub -m 0.01 date
testqpy $QPY_U1 check
print Waiting a bit...
sleep 3
testqpy $QPY_U1 check
runUser $QPY_U1 cat job_1.out
runUser $QPY_U1 cat job_1.err

testqpy $QPY_U1 sub -m 0.01 sleep 20
testqpy $QPY_U1 check

for i in 1 2 3
do
    print Waiting 10 seconds, ${i}/3
    sleep 10
    testqpy $QPY_U1 check
    testqpy $QPY_U1 status
    testqpy $QPY_U1 config
done
print

for i in 1 2 3 4 5
do
    testqpy $QPY_U1 sub -m 0.01 sleep 20
done

for i in 1 2 3 4
do
    echo @ Waiting 10 seconds, ${i}/4
    sleep 10
    testqpy $QPY_U1 check
    testqpy $QPY_U1 status
done

testqpy $QPY_U1 clean all
testqpy $QPY_U1 check

for i in 1 2 3 4 5
do
    testqpy $QPY_U1 sub -m 0.01 sleep 20
    testqpy $QPY_U2 sub -m 0.01 sleep 20
done

for i in 1 2 3 4 5 6
do
    print Waiting 10 seconds, ${i}/6
    sleep 10
    testqpy $QPY_U1 check
    testqpy $QPY_U2 check
    testqpy $QPY_U3 check
    testqpy_multiuser status
done

finish_test $QPY_U1 $QPY_U2 $QPY_U3

