#
# Basic functions for a test environment for qpy
#
# TODO: get outputs from calls to qpy, analyse and check
#       for errors
#
#
QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd .. && pwd )"
QPY_MU_DIR="${HOME}/.qpy-multiuser-test"
exe_QPY_MU="python ${QPY_SOURCE_DIR}/qpy-access-multiuser.py"
exe_QPY="python ${QPY_SOURCE_DIR}/qpy"

normal=$(tput sgr0)
bold=`tput bold`
standout=`tput smso`
offstandout=`tput rmso`
underline=`tput smul`
offunderline=`tput rmul`
bgred=$(tput setab 1)
bgwhite=$(tput setab 7)

function print(){
    echo "${bold}qpy_test>${normal} $@"
}

function printU(){
    u=$1; shift
    echo "${bold}($u)>${normal} ${bgwhite}$@${normal}"
}

function printMU(){
    echo "${bold}(multiuser)>${normal} ${bgwhite}$@${normal}"
}

function printSep(){
    print "-------------------------------------------------------"
}

function testHeader(){
    printSep
    print Test run for qpy.
    print
    print Test $1
    shift
    print $@
    printSep
}

function makeTestDir(){
    if [[ -d ${QPY_MU_DIR} ]]
    then
	print Found directory ${QPY_MU_DIR}. Removing it.
	rm -rf ${QPY_MU_DIR}
    fi
    mkdir ${QPY_MU_DIR}
}

function checkIsTestRunning(){
    if [[ -f ${QPY_SOURCE_DIR}/test_dir ]]
    then
	print We found ${QPY_SOURCE_DIR}/test_dir
	print Are you already testing qpy?
	print Mission aborted.
	exit 1
    fi
    touch ${QPY_SOURCE_DIR}/test_dir
}


# Wrapper to qpy functions
function runUser(){
    user=$1; shift
    printU $user $@
    $@
}


function testqpy_multiuser(){
    printMU qpy-multiuser $@
    $exe_QPY_MU $@
}

function testqpy(){
    user=$1; shift
    qpy_option=$1; shift
    export QPY_TEST_USER=${user};
    printU $user qpy $qpy_option $@
    #$exe_QPY $qpy_option $@

    ## Why this does not work with qpy restart?
    ## It seems that the output from qpy-master is still connected
    ## to the pipe
    #stdbuf -oL -eL $exe_QPY $qpy_option $@ 2> /dev/stdout | tee tmp_abc
    #echo '---'
    #cat tmp_abc

}

function finish_test(){
    print Finishing the test
    for u in $@
    do
	testqpy ${u} finish
    done
    testqpy_multiuser finish
    rm ${QPY_SOURCE_DIR}/test_dir
    print Done!
}

