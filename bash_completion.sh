# Tab completion generator for qpy
# 
# 29 May 2015 - Pradipta and Yuri
_qpy() 
{
    local cur prev opts job_kind
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    opts="sub check ctrlQueue kill restart finish nodes notes status maxJobs config clean tutorial"

    opts_nodes="add remove forceRemove"

    jobID="__job_ID__"
    newnode="__new_node__"
    maxJ="__max_jobs__"
    pattern="__pattern__"
    check_fmt="__quoted_fmt__"
    noArg="__no_arguments__"
    note="__note__"

    keys_all="all"
    keys_maxJob_default="maxJob_default"
    keys_status="queue running done killed undone"
    keys_config="checkFMT copyScripts colour coloursScheme"
    keys_unfinished="queue running"
    keys_finished="done killed undone"
    keys_ctrlQueue="pause continue jump"
    keys_TorF="true false"
    keys_colours="yellow blue green red grey magenta cyan white"

    qpy_dir=$( dirname "${BASH_SOURCE[0]}" )

    file_curnodes="${qpy_dir}/.current_nodes"
    file_knownnodes="${qpy_dir}/.known_nodes"

# Interactive documentation 
    question_ASCII=$(printf "%d" "'?")

    if [[ ${COMP_TYPE} == ${question_ASCII} && ${cur} == '?' ]] ; then

	echo 

	if [[ ${prev} == 'qpy' ]] ; then
	
	    echo "The first argument must be an option."
	else

	    if [[ ${COMP_WORDS[@]} > 2 ]]
	    then
		job_kind="${COMP_WORDS[1]}"
	    fi
	    
	    echo ${job_kind}:

	    case ${job_kind} in
		
                # ==========
 		finish)
		    echo "Finishes the execution of qpy-master."
		    ;;
                # ==========
 		kill)
		    echo "Kill the required jobs."
		    ;;
                # ==========
                status)
                    echo "Show the multiuser status."
                    ;;
                # ==========
                restart)
                    echo "Restart qpy-master."
                    ;;
                # ==========
 		sub)
		    echo "Submit a job."
		    ;;
                # ==========
                ctrlQueue)
                    echo "Fine control over the queue."
                    ;;
                # ==========
 		clean)
		    echo "Remove finished jobs jobs from the list."
		    ;;
                # ==========
 		nodes)
		    echo "Show information about, add or remove nodes."
		    ;;
                # ==========
 		notes)
		    echo "Add or show notes to job."
		    echo "You can add a note with several lines using quotes"
		    ;;
                # ==========
 		config)
		    echo "Show or change current configuration for qpy."
		    ;;
                # ==========
 		check)
		    echo "Give a list of (required) jobs."
		    ;;
                # ==========
 		maxJobs)
		    echo "Show/change maximum jobs per node."
		    ;;
                # ==========
 		tutorial)
		    echo "Open the tutorial."
		    ;;
		*)
		    echo "Unknown option."
		    ;;
	    esac
	fi
	
	echo "---"
	echo "For a brief description of the options call qpy without options."
	echo "For more information try the turorial." | tr -d '\n'

	COMPREPLY=( $(compgen -W "---") )
        return 0
    fi



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

	# ==========
	finish)

	    COMPREPLY=( $(compgen -W ": ${noArg}") )

	    compopt +o nospace
	    return 0;;

	# ==========
	kill)

	    COMPREPLY=( $(compgen -W "${keys_all} ${keys_unfinished}" ${cur}) )
	    if [[ "x${cur}" == 'x' ]] ; then
		COMPREPLY=("${COMPREPLY[@]}" "${jobID}")
	    fi

	    compopt +o nospace
            return 0;;

	# ==========
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

	# ==========
	clean)

	    COMPREPLY=( $(compgen -W "${keys_finished} all" ${cur}) )
	    if [[ "x${cur}" == 'x' ]] ; then
		COMPREPLY=("${COMPREPLY[@]}" "${jobID}")
	    fi

	    compopt +o nospace
            return 0;;


	# ==========
	ctrlQueue)

	    COMPREPLY=( $(compgen -W "${keys_ctrlQueue}"  ${cur}) )

	    action="${COMP_WORDS[2]}"

	    if [[ ${action} == 'jump' ]] ; then
		if [[ "x${cur}" == 'x' && "${prev}" == 'jump' ]] ; then
		    COMPREPLY=( $(compgen -W ": ${jobID}"))
		else
		    COMPREPLY=( $(compgen -W ": ${jobID} begin end" $cur))
		fi
	    elif [[ ${action} == 'pause' || ${action} == 'continue' ]] ; then
		COMPREPLY=( $(compgen -W ": ${noArg}") )
	    fi

	    compopt +o nospace
	    return 0;;


	# ==========
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

		if [[ -z "${nodes}" ]] ; then
		    COMPREPLY=( $(compgen -W ": ${newnode}" ${cur}) )
		else

		    if [[ "x${cur}" == 'x' ]] ; then
			COMPREPLY=( $(compgen -W "${nodes} ${newnode}" ${cur}) )
		    else
			COMPREPLY=( $(compgen -W "${nodes}" ${cur}) )
		    fi
		fi

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

	# ==========
	config)

	    COMPREPLY=( $(compgen -W "${keys_config}"  ${cur}) )

	    action="${COMP_WORDS[2]}"

	    if [[ ${action} == 'checkFMT' ]] ; then
		COMPREPLY=( $(compgen -W "default ${check_fmt}" ${cur}) )
		if [[ -z "${COMPREPLY}" ]] ; then
		    COMPREPLY=( $(compgen -W ": ${check_fmt}") )
		fi
	    fi  

	    if [[ ${action} == 'copyScripts' ]] ; then
		COMPREPLY=( $(compgen -W "${keys_TorF}" ${cur}) )
		if [[ -z "${COMPREPLY}" ]] ; then
		    COMPREPLY=( $(compgen -W ": ${check_fmt}") )
		fi
	    fi

	    if [[ ${action} == 'colour' ]] ; then
		COMPREPLY=( $(compgen -W "${keys_TorF}" ${cur}) )
		if [[ -z "${COMPREPLY}" ]] ; then
		    COMPREPLY=( $(compgen -W ": ${check_fmt}") )
		fi
	    fi

	    if [[ ${action} == 'coloursScheme' ]] ; then
		if [ ${#COMP_WORDS[@]} -lt 9 ]; then
		    COMPREPLY=( $(compgen -W "${keys_colours}" ${cur}) )
		else
		    COMPREPLY=( $(compgen -W ": ${noArg}") )
		fi
	    fi

	    compopt +o nospace
	    return 0;;
	
	# ==========
	check)

            COMPREPLY=( $(compgen -W "${keys_status}" ${cur}) )

	    compopt +o nospace
            return 0;;


	# ==========
	notes)

	    if [ ${#COMP_WORDS[@]} -eq 3 ]; then
		COMPREPLY=( $(compgen -W ": ${jobID}") )
	    else
		COMPREPLY=( $(compgen -W ": ${note}") )
	    fi

	    compopt +o nospace
            return 0;;

	# ==========
	maxJobs)
	    if [[ ${prev} == 'maxJobs' ]] ; then

		COMPREPLY=( $(compgen -W ": 0 1 2 3 ${maxJ}") )

	    else

		nodes=''
		for n in `cat $file_curnodes`
		do
		    nodes="$nodes $n"
		done

		if [[ -n "${nodes}" ]] ; then
		    COMPREPLY=( $(compgen -W "${nodes} ${keys_maxJob_default}" ${cur}) )
		fi

	    fi

	    compopt +o nospace
	    return 0;;

	# ==========
	tutorial)

	    if [[ ${prev} == 'tutorial' ]] ; then

		COMPREPLY=( $(compgen -W "${opts}" ${cur}) )

		if [[ "x${cur}" == 'x' ]] ; then
		    COMPREPLY=("${COMPREPLY[@]}" "${pattern}")
		else
		    if [[ -z "${COMPREPLY}" ]] ; then
			COMPREPLY=( $(compgen -W ": ${pattern}") )
		    fi
		fi
	    else
		COMPREPLY=( $(compgen -W ": ${pattern}") )
	    fi
	    
	    compopt +o nospace
	    return 0;


    esac

}
complete -o nospace -F _qpy qpy
