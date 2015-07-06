# Tab completion generator for qpy
# 
# 29 May 2015 - Pradipta and Yuri
_qpy() 
{
    local cur prev opts job_kind
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    opts="sub check kill finish nodes maxJobs config clean tutorial"

    opts_nodes="add remove forceRemove"

    jobID="__job_ID__"
    newnode="__new_node__"

    keys_all="all"
    keys_status="queue running done killed undone"
    keys_unfinished="queue running"
    keys_finished="done killed undone"

    qpy_dir=$( dirname "${BASH_SOURCE[0]}" )

    file_curnodes="${qpy_dir}/.current_nodes"
    file_knownnodes="${qpy_dir}/.known_nodes"

    # The main option
    if [[ ${prev} == 'qpy' ]] ; then
        COMPREPLY=( $(compgen -W "${opts}" ${cur}) )
	compopt +o nospace
        return 0
    fi

    if [[ ${COMP_WORDS[@]} > 2 ]]
    then
	job_kind="${COMP_WORDS[1]}"
    fi


    case ${job_kind} in
	sub)
	    # an executable
	    if [[ ${prev} == 'sub' ]] ; then
		COMPREPLY=( $(compgen -c ${cur}) )
		compopt +o nospace
		return 0
	    fi
	    
	    # a file
            COMPREPLY=( $(compgen -o plusdirs -f ${cur}) )
	    if [ ${#COMPREPLY[@]} -eq 1 ]; then
		[ -d "$COMPREPLY" ] && LASTCHAR=/
		COMPREPLY=$(printf %q%s "$COMPREPLY" "$LASTCHAR")
	    else
		for ((i=0; i < ${#COMPREPLY[@]}; i++)); do
		    [ -d "${COMPREPLY[$i]}" ] && COMPREPLY[$i]=${COMPREPLY[$i]}/
		done
	    fi
	    return 0;;
	
	check)
            COMPREPLY=( $(compgen -W "${keys_status}" ${cur}) )
	    compopt +o nospace
            return 0;;

	kill)
	    COMPREPLY=( $(compgen -W "${keys_all} ${keys_unfinished}" ${cur}) )
	    if [[ "x${cur}" == 'x' ]] ; then
		COMPREPLY=("${COMPREPLY[@]}" "${jobID}")
	    fi
	    compopt +o nospace
            return 0;;
	
#	maxJobs) Doesn't work...
#	    if [[ ${prev} == 'maxJobs' ]] ; then
#		COMPREPLY=( $(compgen -W "<node> <n_jobs>" ${cur}) )
#	    fi
#            compopt +o nospace
#            return 0;;
    
	nodes)
	    if [[ ${prev} == 'nodes' ]] ; then
		COMPREPLY=( $(compgen -W "${opts_nodes}" ${cur}) )
		compopt +o nospace
		return 0
	    fi

	    action="${COMP_WORDS[2]}"

	    if [[ ${action} == 'add' ]] ; then

		cur_nodes='::'
		for n in `cat $file_curnodes`
		do
		    cur_nodes="$cur_nodes::$n::"
		done

		nodes=''
		for n in `cat $file_knownnodes`
		do
		    if [[ ! ( ${cur_nodes} =~ "::$n::") ]] ; then
			nodes="$nodes $n"
		    fi
		done

		COMPREPLY=( $(compgen -W "${nodes} ${newnode}" ${cur}) )

	    fi

	    if [[ ${action} == 'remove' || ${action} == 'forceRemove' ]] ; then

		nodes=''
		for n in `cat $file_curnodes`
		do
		    nodes="$nodes $n"
		done

		if [[ -n "${nodes}" ]] ; then
		    COMPREPLY=( $(compgen -W "${nodes} ${keys_all}" ${cur}) )
		fi

	    fi

	    compopt +o nospace
	    return 0;;

	clean)
	    COMPREPLY=( $(compgen -W "${keys_finished} all" ${cur}) )
	    if [[ "x${cur}" == 'x' ]] ; then
		COMPREPLY=("${COMPREPLY[@]}" "${jobID}")
	    fi
	    compopt +o nospace
            return 0;;

	tutorial)
	    if [[ ${prev} == 'tutorial' ]] ; then
		COMPREPLY=( $(compgen -W "${opts}" ${cur}) )
		if [[ "x${cur}" == 'x' ]] ; then
		    COMPREPLY=("${COMPREPLY[@]}" "<pattern>")
		fi
		compopt +o nospace
		return 0;
	    fi

    esac

}
complete -o nospace -F _qpy qpy
