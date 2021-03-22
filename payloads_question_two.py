# Answer to the first question

### Question 1
'''
Write an HTTP server that exposes a `/stats` route. The server must be able to : 

- Receive the JSON payloads sent by Streamroot-powered devices
- Write the data into a postgreSQL database (in a `stats` table). Each row in the table should be the aggregation of payloads received during a **5 minutes time window**. The table should have the following columns :

    * **Time** (datetime) : the timestamp of **the end** of the 5-minute window

    * **customer** (string) : the customer aggregated in that row

    * **content** (string) : the content aggregated in that row

    * **cdn** (integer) : the **sum** (in bytes) of all the data dwonloaded via the CDN during the time window (for the corresponding content and customer)

    * **p2p** (integer) : the **sum** (in bytes) of all the data downloaded via P2p during the time window (for the corresponding content and customer)
'''

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from datetime import datetime, timedelta

import time
import glob 
from threading import Thread
import json
import os
import pandas as pd

#Function to calculate the interval between two dates, this function return a tuple of minutes and seconds 

def calculate_interval(min_date, max_date) :
    difference = max_date - min_date
    seconds_in_day = 24 * 60 * 60
    timedelta(0, 8, 562000)
    return list(divmod(difference.days * seconds_in_day + difference.seconds, 60))

#Function that works as a listner on the background to a folder where we store the payloads received from the stats service
#This function aggregate the files by the customer id and the content id and then calculate the interval between the recent file et the old one
#If the interval is greater than 5, store the files loaded (json) into a dataframe and create a new record based on the stats model 
def process_payloads() :

# An infinite loop, with a sleep period of 5 seconds
    while True :
        try :
			# Store the filenames on a pandas dataframe
            df = pd.DataFrame(glob.glob('[THE_PATH_TO_THE_PAYLOADS/*'), columns = ['FileName'])
            #Parse the file name
            df[['Customer', 'Content', 'EndDate']] = df['FileName'].str.split('|',expand=True)[[1, 2, 3]] 
            df['EndDate'] = pd.to_datetime(df['EndDate'])
            for _, group in df.groupby(['Customer', 'Content']) :
                interval = calculate_interval(group['EndDate'].min(), group['EndDate'].max())
                if (interval[0]>=5) :
                    payloads_list=list()
                    for _, payload in group.iterrows() :
                        with open(payload['FileName'], 'r') as f :
                            data = json.load(f)
                            print([data['customer'], data['content'], data['p2p'], data['cdn'], data['token']])
                            payloads_list.append([data['customer'], data['content'], data['p2p'], data['cdn'], data['token']])
                    payloads = pd.DataFrame(payloads_list, columns=['customer', 'content', 'p2p', 'cdn', 'token'])
                    #print([payloads['customer'].values[0], payloads['content'].values[0], payloads['p2p'], payloads['cdn'].sum(), payloads['token'].sum()])
                    stats = StatssModel(time = group['EndDate'].max(),
                                        customer = payloads['customer'].values[0],
                                        content = payloads['content'].values[0],
                                        p2p = int(payloads['p2p'].sum()),
                                        cdn = int(payloads['cdn'].sum()),
										sessions = int(payloads['token'].nunique())) # calculate the nbumber of unique tokens into the interval of 5 minutes 
					#Insert and commit into the database, if everything ok remove the files 
                    db.session.add(stats)
                    db.session.commit()
                    for file in group['FileName'].to_list() :
                        os.remove(file)
        except Exception as e :
            print("Can't payloads. Error : " + str(e))
            pass
        time.sleep(5)

# declare the app Flask
        
app = Flask(__name__)

# Add configuration of the database 
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql+psycopg2://[USERNAME]:[PASSWORD]@[HOST]:[PORT]/[DB_NAME]"
db = SQLAlchemy(app)
migrate = Migrate(app, db)

#The stats Model 
class StatssModel(db.Model):
    __tablename__ = 'stats'
    
    id = db.Column(db.Integer, primary_key=True)
    time = db.Column(db.DateTime())
    customer = db.Column(db.String())
    content = db.Column(db.String())
    p2p = db.Column(db.Integer())
    cdn = db.Column(db.Integer())
	# Add sessions 
    sessions = db.Column(db.Integer())
#     sessionDuration = db.Column(db.Integer())

    def __init__(self, time, customer, content, p2p, cdn, sessions):
        self.time = time
        self.customer = customer
        self.content = content
        self.p2p = p2p
        self.cdn = cdn
		self.sessions = sessions

# Exposed service, that take only POST requests  
@app.route('/stats', methods=['POST'])
def serialize_payload():
    if request.method == 'POST':
        if request.is_json:
            data = request.get_json()
            with open('payloads/|'+ data['customer']+'|'+ data['content'] + '|' + str(datetime.now())+'|.json', 'w') as f:
                json.dump(data, f)
            return '1'
        else:
            return '0'
    else :
        return '0'

    
if __name__ == '__main__':
    # run app in debug mode on port 5000
    thread = Thread(target=process_payloads)
    thread.daemon = True
    thread.start()
    app.run(debug=True, port=5000)
