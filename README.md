# Recipe for running CEDA FBS on the whole archive

## Login to jasmin-sci2 server and locate yourself

```
$ ssh ${USER}@jasmin-sci2.ceda.ac.uk
$ cd /group_workspaces/jasmin/cedaproc/${USER}/
$ mkdir fbs
$ export BASEDIR=$PWD
$ cd fbs/
```

## Get and install ceda-fbs code from Git (with install script)

```
$ wget https://raw.githubusercontent.com/cedadev/ceda-fbs/master/install-ceda-fbs.sh
$ .  ./install-ceda-fbs.sh
```

This will build you a `virtualenv` locally so your environment should look like:

```
$ ls
ceda-fbs  install-ceda-fbs.sh  venv-ceda-fbs
```

## Create a little setup script

```
$ cat setup_env.sh
export BASEDIR=/group_workspaces/jasmin/cedaproc/$USER/fbs
export PYTHONPATH=$BASEDIR/ceda-fbs/python:$BASEDIR/ceda-fbs/python/src/fbs
. venv-ceda-fbs/bin/activate
```

## Configure servers and Elasticsearch index

You need to tell `ceda-fbs` some key things in the config file (ceda_fbs.ini) at:

`$BASEDIR/ceda-fbs/python/config/ceda_fbs.ini`

You will need to edit the following sections:

```
 log-path - make it local to the cedaproc area						
 es-host = jasmin-es1ceda.ac.uk.							
 es-index = ceda-archive-level-2							
 es-index-settings = /group_workspaces/jasmin/cedaproc/__INSERT_USERID_HERE__/fbs/ceda-fbs/elasticsearch/mapping/level_3_settings_1.json	
 num-files = 10000		
 num-processes = 128		
```

NOTE: change `__INSERT_USERID_HERE__` to your userid.

## 1. Scan the file system for a list of all CEDA datasets

```
$ ceda-fbs/python/src/fbs/gui/create_datasets_ini_file.sh
Wrote datasets file to: ceda_all_datasets.ini
```

You should now have a file mapping identifiers to dataset paths, i.e.:

```
$ head -3 ceda_all_datasets.ini
badc__abacus=/badc/abacus/data
badc__accacia=/badc/accacia/data
badc__accmip=/badc/accmip/data
```


