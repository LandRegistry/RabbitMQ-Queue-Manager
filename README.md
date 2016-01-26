# RabbitMQ Queue Manager 

This is the repo for the land registry Queue Manager. It is written in Python, with the Flask framework.  

The code is derived from that of https://github.com/LandRegistry/register-publisher and is intended to be used as a Git Submodule.


## Setup

To create a virtual env, run the following from a shell:

```  
    mkvirtualenv -p /usr/bin/python3 queue-manager
    source environment.sh
    pip install -r requirements.txt
```

## Run unit tests

To run unit tests for the queue-manager, go to its folder and run `lr-run-tests`.

