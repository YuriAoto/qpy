# Tab completion generator for qpy
#
# 29 May 2015 - Pradipta and Yuri
_qpy() 
{
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    prev_2="${COMP_WORDS[COMP_CWORD-2]}"
    opts="sub check kill finish nodes njobs"

    if [[ ${prev} == 'qpy' ]] ; then
        COMPREPLY=( $(compgen -W "${opts}" ${cur}) )
        return 0
    fi

    if [[ ${prev} == 'sub' ]] ; then
        COMPREPLY=( $(compgen -c ${cur}) )
        return 0
    fi

    if [[ ${prev_2} == 'sub' ]] ; then
        COMPREPLY=( $(compgen -f ${cur}) )
        return 0
    fi

}
complete -F _qpy qpy
