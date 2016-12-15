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
export PYTHONPATH=$BASEDIR/ceda-fbs/python:$BASEDIR/ceda-fbs/python/src/fbs:$PYTHONPATH
export PATH=$PATH:$BASEDIR/ceda-fbs/python/src/fbs/gui
. venv-ceda-fbs/bin/activate
```

## Configure servers and Elasticsearch index

You need to tell `ceda-fbs` some key things in the config file (ceda_fbs.ini) at:

`$BASEDIR/ceda-fbs/python/config/ceda_fbs.ini`

You will need to edit the following sections:

```
 log-path = /group_workspaces/jasmin/cedaproc/__INSERT_USERID_HERE__/fbs/log
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

You should now have an INI file that maps identifiers to dataset paths, i.e.:

```
$ head -3 ceda_all_datasets.ini
badc__abacus=/badc/abacus/data
badc__accacia=/badc/accacia/data
badc__accmip=/badc/accmip/data
```

## 2. Create file lists for every dataset (ready for the actual scanning)

Make directories ready for file lists and log files:

```
$ mkdir logs datasets
```
!WARNING: files lists can be *many Gbytes in size* so don't do this in your home directory.

Now run the first LOTUS jobs to generate lists of all files in each dataset.

```
$ python make_file_lists.py -f ceda_all_datasets.ini -m $JRAINNIE/fbs/datasets --host lotus -p 256
```

This will submit lots of jobs to LOTUS...and wait...and try to submit more.

## 3. Execute the scan commands on LOTUS

Before you do this: Create: `~/.forward` (containing just your email address) - so that LOTUS messages will be mailed to you.

Next, run the `run_commands_in_lotus.py` script to work its way through the list of commands inside the `lotus_commands.txt` file by submitting up to 128 at any one time.

On `jasmin-sci[12].ceda.ac.uk`, run:

```
$ nohup python run_commands_in_lotus.py -f lotus_commands.txt -p 128 > /dev/null 2>&1 &
```

## 4. Watch the file count building

You can see how things are progressing in the web-interface:

 http://jasmin-es1.ceda.ac.uk:9200/_plugin/head/
 
Or, you can use the `Sense` plugin in Chrome, and try: 

`GET jasmin-es1.ceda.ac.uk:9200/ceda-archive-level-2/_count`

## 5. Set the Index to NOT use replica shards

Using `curl`, `wget` or the `Sense` plugin, call:

```
PUT jasmin-es1.ceda.ac.uk:9200/ceda-di-testing/_settings
{
    "index.routing.allocation.disable_allocation": false
}
```
