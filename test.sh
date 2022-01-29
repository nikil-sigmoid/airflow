COMMIT_MSG="dsf"

while getopts m:cph option
do 
    case "${option}" in

        m)
			COMMIT_MSG=${OPTARG}
			echo $COMMIT_MSG
			;;
        c)	
			# CREATE_PUSH="true"
			;;
        p)	
			# JUST_PUSH="true"
			;;
		h|*)
			echo "-h				For help."
			echo "-c    			To create remote repo and commit and push local repo."
			echo "-p    			To commit and push local repository."
			echo "-m <commit_msg>   Write your commit message(necessary)."
			;;
    esac
done


echo $COMMIT_MSG


if [ "great nice" == "${COMMIT_MSG}" ];  
then  
echo "Both the strings are equal."  
else  
echo "Strings are not equal."  
fi  