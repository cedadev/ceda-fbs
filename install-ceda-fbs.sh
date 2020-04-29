#!/bin/bash

code_dir=$PWD/ceda-fbs
env_dir=$PWD/venv-ceda-fbs

print_usage() 
{
  echo -e "\n\nUsage: install-ceda-fbs.sh  -c /dir/to/install/code/ -e /dir/to/install/venv/"
  echo -e "if arguments not specified the current folder will be used.\n\n"   
}

install_ceda_fbs()
{
  echo     "**********************************************"
  echo     "**Installation of ceda-fbs project on jasmin.**"
  echo -e  "**********************************************\n"

  echo    "***********************"
  echo    "**Cloning repository.**"
  echo    "***********************"

  git clone https://github.com/cedadev/ceda-fbs $code_dir 
  
  echo "***************************"
  echo "**Installing environment.**"
  echo "***************************"

  python -m venv $env_dir --system-site-packages
  . $env_dir/bin/activate
  pip install -r  $code_dir/python/pip_requirements.txt
  pip install $code_dir

  echo  "***********************"
  echo  "**Installation ended.**"
  echo  "***********************"
}

# main program...
code_dir_set=0
env_dir_set=0

# parse flags
while getopts ':c:he:' OPT; do
 case $OPT in
  c) code_dir="$OPTARG" && code_dir_set=1;;    #<code_dir>: where the code will be installed.
  e) env_dir="$OPTARG" && env_dir_set=1;;      #<env_dir> : where the env will be installed.
  h) print_usage && exit 0;;                   #          : displays this help
  \?) echo "Unknown option '$OPTARG'" >&2 && print_usage && exit 1;;
  \:) echo "Missing parameter for flag '$OPTARG'" >&2 && print_usage && exit 1;;
 esac
done

shift $(($OPTIND - 1))

#echo $code_dir
#echo $env_dir
#echo "$#"

if    ( [ "$code_dir_set" -eq 1 ] &&  [ "$env_dir_set" -eq 0 ] )\
   || ( [ "$code_dir_set" -eq 0 ] &&  [ "$env_dir_set" -eq 1 ] );\
then
 print_usage
 exit
elif ( [ "$code_dir_set" -eq 0 ] &&  [ "$env_dir_set" -eq 0 ] && [ "$#" -eq 0 ]); #the default case.
then
  install_ceda_fbs
elif ( [ "$code_dir_set" -eq 1 ] &&  [ "$env_dir_set" -eq 1 ] && [ "$#" -eq 0 ]);#the case when directories are specified.
then
  install_ceda_fbs
else
 print_usage
fi

