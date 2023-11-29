import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the tables
tbl_measurement=Base.classes.measurement
tbl_station=Base.classes.station

# Set the min and max dates to variables
session = Session(engine)
date_max = session.query(tbl_measurement.date).order_by(tbl_measurement.date.desc())[0][0]
session.close()

date_array = date_max.split('-')

# Calculate the date one year from the last date in data set.
# adjust by a day if the original date is Feb 29
if (date_array[1]=='02') & (date_array[2]=='29'):
    date_array[2]=str(int(date_array[2])-1)

#Subtract 1 year for the minimum date
date_min = str(int(date_array[0])-1) + "-" + date_array[1] + "-" + date_array[2]


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")
def prcp():
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return precipitation data"""
    query_prcp = session.query(tbl_measurement.date,tbl_measurement.prcp).filter(tbl_measurement.date.between(date_min,date_max)).filter(tbl_measurement.prcp.isnot(None)).all()

    #close the session
    session.close()

    # Convert the results to a dictionary
    dict_prcp = [{row[0]: row[1]} for row in query_prcp]

    return jsonify(dict_prcp)

@app.route("/api/v1.0/stations")
def stations():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    val_list=[]

    """Return precipitation data"""
    station_list =  session.query(tbl_station).all()
    
    #close the session
    session.close()
    
    for row in station_list:
        temp_dict={"station":row.station,"name":row.name,"latitude":row.latitude,"longitude":row.longitude,"elevation":row.elevation}
        val_list.append(temp_dict)
    
    return jsonify(val_list)

@app.route("/api/v1.0/tobs")
def tobs():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query tobs data for the most frequent station
    query_tobs = session.query(tbl_measurement.date,tbl_measurement.tobs).filter(tbl_measurement.station=='USC00519281').filter(tbl_measurement.date.between(date_min,date_max)).all()

    session.close()

    ret_val = [{"date": row[0], "temperature": row[1]} for row in query_tobs]

    return jsonify(ret_val)

def dateSearch(start,end):
    session = Session(engine)
    
    # Get the top station
    top_station=session.query(tbl_measurement.station,func.count(tbl_measurement.station)).group_by(tbl_measurement.station).order_by(func.count(tbl_measurement.station).desc()).all()[0][0]
    
    # Query tobs data for the most frequent station
    min_temp = session.query(func.min(tbl_measurement.tobs)).filter(tbl_measurement.station==top_station).filter(tbl_measurement.date.between(start,end))[0][0]
    max_temp = session.query(func.max(tbl_measurement.tobs)).filter(tbl_measurement.station==top_station).filter(tbl_measurement.date.between(start,end))[0][0]
    avg_temp = session.query(func.avg(tbl_measurement.tobs)).filter(tbl_measurement.station==top_station).filter(tbl_measurement.date.between(start,end))[0][0]
    ret_dict={"minimum temperature":min_temp,"maximum temperature":max_temp,"average temperature":avg_temp}
    
    return jsonify(ret_dict)
    
    
@app.route("/api/v1.0/<start>")
def startDate(start):

    #run the dateSearch with today as the end time
    return dateSearch(start,dt.date.today())

@app.route("/api/v1.0/<start>/<end>")
def startEndDate(start,end):

    #run the dateSearch with today as the end time
    return dateSearch(start,end)


if __name__ == '__main__':
    app.run(debug=True)
