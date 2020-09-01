from flask import Flask
from flask import request
import subprocess
import time


app = Flask('flaskshell')
query = "pm2 restart 0"
lastRestart=0


def valid_ip():
    client = request.remote_addr
    if client in ip_whitelist:
        return True
    else:
        return False


@app.route('/restart/', methods = ['POST'])
def get_status():
    try:
        global lastRestart
        currentTime=int(round(time.time() ))
        if ((currentTime-lastRestart)>900):
            result_success = subprocess.check_output([query], shell=True)
            lastRestart=currentTime
            return 'Success '+str(currentTime-lastRestart)+" time left"
    except subprocess.CalledProcessError as e:
        return "An error occurred while trying to fetch task status updates."

    return 'Success'



if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=5050)