import sys
import subprocess 
#:: for executing commands in the terminal and deal with results within Python Code!
# Popen :: class for executing terminal commands in form of a list of command and its options
# Hence that every shell command has the following stdin/stderr/stdout the Popen class has arguments for each of them
# PIPE :: usually passed in the in the stdout argument and it means that you are using the output of this command with
# another command or you want to retrieve the output to be used in another stage of your code
from subprocess import PIPE, Popen
import argparse
import os
import json 

# listdir class :: for retrieving all the contents of a directory --> files, dirs
from os import listdir
# Import isfile() :: boolean checker evaluated to True if the passed is file.
# join :: for construction paths by concatenation.
from os.path import isfile, join
import pandas as pd 
import re
from pandas.io.json import json_normalize
from datetime import datetime

#input_path = sys.argv[1]
# list for all files in a directory
startTime = datetime.now()
parser = argparse.ArgumentParser()
parser.add_argument("path",
                    help="Enter path to be start")

parser.add_argument("-u", "--convert", action="store_true", dest="convert", default=False,
                    help="Convert timestamp")
args = parser.parse_args()
files = [item for item in listdir(args.path) if isfile(join(args.path, item)) and (item.endswith('.json')) and 'Done' not in item  ]
# empty dict for checksum and file in a key and value format.
checksums = {}
# empty list for the duplicated checksums
duplicates = []

# Iterate over the list of files
for filename in files:
    
    print(filename)
    # Use Popen to call the md5sum utility
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
        
        checksum = proc.stdout.read().split()[0]
       
        if checksum in checksums:
            duplicates.append(filename)
        checksums[checksum] = filename

print(f"Found Duplicates: {duplicates}")
##remove duplicates
for i in duplicates:
    os.remove(os.path.join('.', i))
##cleaning files##########################

filepaths  = [os.path.join(args.path, name) for name in os.listdir(args.path)if isfile(join(args.path, name)) and (name.endswith('.json'))]
df = []
for file in filepaths:
    if 'Done' not in file :
        records = [json.loads(line) for line in open(file) if ('_heartbeat_' not in json.loads(line))]
    
    #not line.endswith('Done.json') and
    
    

        df = json_normalize(records)
        
        df2=df.dropna()
        dff = df2[['a','tz','u','r','t','hc','cy','ll']]
        data=pd.DataFrame(dff['ll'].to_list(),columns=['longitude','latitude'])
        result = pd.concat([dff, data], axis=1)
        results=result.dropna()
        #result = re.search('asdf=5;(.*)123jasd', s)
        browser=results.a.str.split(' \(', expand=True)[0]
        op_sys=results.a.str.split(' \(', expand=True)[1]
        
        #operating_sys=results.a.apply(lambda st: st[st.find("(")+1:st.find(")")])
        

        final = pd.concat([browser,results,op_sys], axis=1)

        
        res=final.drop(['a', 'll'], axis = 1)

        if args.convert:
            res
        else:    
            res['t'] = pd.to_datetime(res['t'], unit='s')
            res['hc'] = pd.to_datetime(res['hc'], unit='s')




        output=res.rename(columns={'0' : 'browser',
        'tz': 'time_zone', 'r': 'from_url', 
        'u' : 'to_url', 'cy' : 'city' , 't' : 'time_in' , 'hc' : 'time_out',df.columns[1]: 'browser'})
        
        
        
        name, ext = os.path.splitext(os.path.basename(file))
        new_path = os.path.join(args.path, f"{name}Done.json")
        output_filename = os.path.join('./target', f"{name}Output.csv")
        output.to_csv(output_filename,index = False)
        os.rename(file, new_path)
        print(f"file path is :{output_filename}")
       
       
    else:
        print('no new files')
        
     
print(f"execution time is :{datetime.now() - startTime}")
        
       