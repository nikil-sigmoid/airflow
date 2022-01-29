#!/bin/bash

CREATE_PUSH="false"
COMMIT_MSG="no_commit_msg_provided"
JUST_PUSH="false"


while getopts m:cph option
do 
    case "${option}" in

        m)
			COMMIT_MSG=${OPTARG}
			;;
        c)	
			CREATE_PUSH="true"
			;;
        p)	
			JUST_PUSH="true"
			;;
		h|*)
			echo "-h				For help."
			echo "-c    			To create remote repo and commit and push local repo."
			echo "-p    			To commit and push local repository."
			echo "-m <commit_msg>   Write your commit message(necessary)."
			;;
    esac
done


if [ $CREATE_PUSH == "true" ]; then

	if [ $COMMIT_MSG == "no_commit_msg_provided" ]; then
		echo "-m 'commit msg option is neccessary'"
		exit
	fi

	echo "This will create remote repo, commit and push local repo."
	git init
	git add .
	git commit -m $COMMIT_MSG
	gh repo create $(basename "$PWD") --public --source=. --remote=origin --push

elif [ $JUST_PUSH == "true" ]; then

	if [ $COMMIT_MSG == "no_commit_msg_provided" ]; then
		echo "-m 'commit msg option is neccessary'"
		exit
	fi

	echo "This will commit and push local repo."
	git add .
	git commit -m $COMMIT_MSG
	git push origin
fi


