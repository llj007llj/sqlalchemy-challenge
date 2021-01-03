from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

from flask import Flask, jsonify
import datetime as dt

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)
MEASUREMENT = Base.classes.measurement
STATION = Base.classes.station

app = Flask(__name__)

@app.route('/')
def home_page():
    available_routes = [
        '/',
        '/api/v1.0/precipitation',
        '/api/v1.0/stations',
        '/api/v1.0/tobs',
        '/api/v1.0/<start>',
        '/api/v1.0/<start>/<end>'
    ]

    return jsonify(available_routes)

@app.route('/api/v1.0/precipitation')
def precipitation_page():
    DB_SESSION = Session(engine)
    db_data = DB_SESSION.query(MEASUREMENT.date, MEASUREMENT.prcp).all()

    result = {}
    for row in db_data:
        result[row[0]] = row[1]

    return jsonify(result)

@app.route('/api/v1.0/stations')
def stations_page():
    DB_SESSION = Session(engine)
    db_data = DB_SESSION.query(STATION).all()

    result = []
    for row in db_data:
        temp = {}
        for column in row.__table__.columns:
            temp[column.name] = str(getattr(row, column.name))
        
        result.append(temp)

    return jsonify(result)

@app.route('/api/v1.0/tobs')
def tobs_page():
    DB_SESSION = Session(engine)
    stations_counts = DB_SESSION.query(MEASUREMENT.station, func.count(MEASUREMENT.station)) \
                        .group_by(MEASUREMENT.station) \
                        .order_by(func.count(MEASUREMENT.station).desc()) \
                        .first()
    active_station = stations_counts[0]

    last_date_str = DB_SESSION.query(MEASUREMENT.date).order_by(MEASUREMENT.date.desc()).first()[0]
    last_date_obj = dt.datetime.strptime(last_date_str, '%Y-%m-%d')
    last_one_year_date_obj = last_date_obj - dt.timedelta(days=365)
    last_one_year_date_str = last_one_year_date_obj.strftime('%Y-%m-%d')

    db_data = DB_SESSION.query(MEASUREMENT.tobs) \
                            .filter(MEASUREMENT.station == active_station) \
                            .filter(MEASUREMENT.date >= last_one_year_date_str, MEASUREMENT.date <= last_date_str) \
                            .all()
    
    result = []
    for row in db_data:
        result.append(row[0])
        
    return jsonify(result)

@app.route('/api/v1.0/<start>')
def temperature_start(start):
    try:
        DB_SESSION = Session(engine)
        db_data = DB_SESSION.query(func.min(MEASUREMENT.tobs), func.avg(MEASUREMENT.tobs), func.max(MEASUREMENT.tobs)) \
                            .filter(MEASUREMENT.date >= start) \
                            .first()
        
        result = {
            'TMIN': db_data[0],
            'TAVG': db_data[1],
            'TMAX': db_data[2]
        }
        return jsonify(result)

    except Exception as e:
        result = {'error': str(e)}
        return jsonify(result)

@app.route('/api/v1.0/<start>/<end>')
def temperature_start_end(start, end):
    try:
        DB_SESSION = Session(engine)
        db_data = DB_SESSION.query(func.min(MEASUREMENT.tobs), func.avg(MEASUREMENT.tobs), func.max(MEASUREMENT.tobs)) \
                            .filter(MEASUREMENT.date >= start) \
                            .filter(MEASUREMENT.date <= end) \
                            .first()
        

        result = {
            'TMIN': db_data[0],
            'TAVG': db_data[1],
            'TMAX': db_data[2]
        }
        return jsonify(result)

    except Exception as e:
        result = {'error': str(e)}
        return jsonify(result)

if __name__ == '__main__':
    app.run()
