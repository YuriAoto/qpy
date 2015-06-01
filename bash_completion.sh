# Tab completion generator for qpy
# 
# 29 May 2015 - Pradipta and Yuri
_qpy() 
{
    local cur prev opts job_kind
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="sub check kill finish nodes njobs config"

    if [[ ${prev} == 'qpy' ]] ; then
        COMPREPLY=( $(compgen -W "${opts}" ${cur}) )
        return 0
    fi

    if [[ ${prev} == 'sub' ]] ; then
        COMPREPLY=( $(compgen -c ${cur}) )
        return 0
    fi

    if [[ ${COMP_WORDS[@]} > 2 ]]
    then
	job_kind="${COMP_WORDS[1]}"
	if [[ ${job_kind} == 'sub' ]] ; then
            COMPREPLY=( $(compgen -o plusdirs -f ${cur}) )

	    if [ ${#COMPREPLY[@]} -eq 1 ]; then
		[ -d "$COMPREPLY" ] && LASTCHAR=/
		COMPREPLY=$(printf %q%s "$COMPREPLY" "$LASTCHAR")
	    else
		for ((i=0; i < ${#COMPREPLY[@]}; i++)); do
		    [ -d "${COMPREPLY[$i]}" ] && COMPREPLY[$i]=${COMPREPLY[$i]}/
		done
	    fi

            return 0
	fi
    fi


}
complete -o nospace -F _qpy qpy
