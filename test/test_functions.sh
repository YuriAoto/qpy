#
# Basic functions for a test environment for qpy
#
# TODO: get outputs from calls to qpy, analyse and check
#       for errors
#
#
QPY_SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1; cd ../src/ && pwd )"
QPY_MU_DIR="${HOME}/.qpy-multiuser-test"
exe_QPY_MU="python ${QPY_SOURCE_DIR}/qpy-access-multiuser.py"
exe_QPY="python ${QPY_SOURCE_DIR}/../qpy"

normal=$(tput sgr0)
bold=`tput bold`
standout=`tput smso`
offstandout=`tput rmso`
underline=`tput smul`
offunderline=`tput rmul`
bgred=$(tput setab 1)
bgwhite=$(tput setab 7)


# =============
# Important dir
function qpyDir(){
    user=$1; shift
    echo "${HOME}/.qpy-test_${user}"
}

function qpyWdir(){
    user=$1; shift
    echo "${user}-wdir/"
}


# ===============
# print functions
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

function printBox(){
    printSep
    print $@
    printSep
}

function testHeader(){
    printSep
    print Test run for qpy.
    print
    print '-->' $1 '<--'
    shift
    print $@
    printSep
}

function showMUlog(){
    printMU cat ${QPY_MU_DIR}/multiuser.log '#' The log file
    cat ${QPY_MU_DIR}/multiuser.log
}

function showUlog(){
    user=$1; shift
    Udir=`qpyDir ${user}`
    printU $user cat ${Udir}/master.log '#' The log file
    cat ${Udir}/master.log
}


# ===============
# Init functions
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

function createUser(){
    user=$1; shift
    QPY_Udir=`qpyDir $user`
    QPY_Wdir=`qpyWdir $user`
    print Creating user $user
    if [[ ! -d ${QPY_MU_DIR} ]]
    then
	print Please, start qpy-multiuser before creating users.
	print Aborting...
	exit
    fi
    if [[ -d ${QPY_Udir} ]]
    then
	print Found directory ${QPY_Udir}. Removing it.
	rm -rf ${QPY_Udir}
    fi
    if [[ -d ${QPY_Wdir} ]]
    then
	print Found directory ${QPY_Wdir}. Removing it.
	rm -rf ${QPY_Wdir}
    fi
    mkdir ${QPY_Udir}
    mkdir ${QPY_Wdir}
    cp ${QPY_MU_DIR}/multiuser_connection_address ${QPY_Udir}
    cp ${QPY_MU_DIR}/multiuser_connection_conn_key ${QPY_Udir}
    cp ${QPY_MU_DIR}/multiuser_connection_port ${QPY_Udir}
    echo ${user} >> ${QPY_MU_DIR}/allowed_users
}



# ===============
# Function to execute something
function testqpy_multiuser(){
    printMU qpy-multiuser $@
    $exe_QPY_MU $@
}

function runUser(){
    user=$1; shift
    (cd `qpyWdir ${user}`
     printU $user $@
     $@
    )
}

function testqpy(){
    user=$1; shift
    (cd `qpyWdir ${user}`
     qpy_option=$1; shift
     export QPY_TEST_USER=${user};
     printU $user qpy $qpy_option $@
     $exe_QPY $qpy_option $@
    )

    ## Why this does not work with qpy restart?
    ## It seems that the output from qpy-master is still connected
    ## to the pipe
    #stdbuf -oL -eL $exe_QPY $qpy_option $@ 2> /dev/stdout | tee tmp_abc
    #echo '---'
    #cat tmp_abc

}

# ===============
# End functions
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

function finish_users_test(){
    print Finishing the test
    for u in $@
    do
	testqpy ${u} finish
    done
    rm ${QPY_SOURCE_DIR}/test_dir
    print Done!
}
