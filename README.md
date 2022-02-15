 #  YACS- Yet Another Centralised Scheduler
 
 This repository contains the code, and report for a Python-based tool to manage the resources and simulate centralized scheduling policies in a distributed setting.
 
 ## **Requirements**
 Install the following if not present, using the command:<br/> `pip install <module name>`
1. json
2. socket
3. time
4. sys
5. random
6. numpy
7. re

## **Steps to execute**
Run the following commands in 5 different terminals: <br/>
1. **Scheduling algorithms:<br/>** `python master.py "config.json" <scheduling algorithm>`<br/> 
  	* RR      : Round Robin<br/>
  
 	* LL      : Least Loaded<br/>
  
  	* RANDOM<br/>
2. `python worker.py 4000 1`<br/>
3. `python worker.py 4001 2`<br/>
4. `python worker.py 4002 3`<br/>
5. `python requests.py <no of requests>`<br/>
6. Run all the above mentioned scheduling algorithms, and save the log files in folders named: RR, LL and  RANDOM<br/>
7. To view the graphs and analyse the various scheduling algorithms, run the command: <br/>
   `python analysis.py`
