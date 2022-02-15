# YACS- Yet Another Centralised Scheduler

This repository contains the code, and report for a Python-based tool to manage the resources and simulate centralized scheduling policies in a distributed setting.

### Requirements:
Install the following modules if not present, using the command:<br/> `pip install <module name>`
<br/>1. json<br/>2. socket<br/>3. time<br/>4. sys<br/>5 random<br/>6. numpy<br/>7. re<br/>

### Steps to execute:
Run the following commands in 5 different terminals:<br/>
a. `python master.py "config.json" <scheduling algorithm>`<br/>
    Scheduling algorithms:<br/>
        i)   RR      : Round Robin<br/>
        ii)  LL      : Least Loaded<br/>
        iii) RANDOM<br/>
b. `python worker.py 4000 1`<br/>
c. `python worker.py 4001 2`<br/>
d. `python worker.py 4002 3`<br/>
e. `python requests.py <no of requests>`<br/>
f. Run all the above mentioned scheduling algorithms, and save the log files in folders named: RR, LL and  RANDOM<br/>
g. To view the graphs and analyse the various scheduling algorithms, run the command: <br/>
   `python analysis.py`
