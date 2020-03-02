from flightstats import FlightstatsAPI
from flightstats import FlightstatsError
from apiclient import RateLimiter

import argparse
import csv
from datetime import datetime  
from datetime import timedelta
import json
from multiprocessing.pool import ThreadPool
import os
import pandas

DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_NUM_HOURS = 6
MIN_DATE = '2006-02-07'
THREADED = True
PATH_TEMPLATE = '{departure_airport_code}/dep/{year}/{month}/{day}/{hourOfDay}'

# env variables from flightstats.com
api_key = os.getenv('FLIGHT_STATS_KEY')
api_id = os.getenv('FLIGHT_STATS_ID')

if not api_key or not api_id:
    print("Missing environment variables")
    print("Provide FLIGHT_STATS_KEY and FLIGHT_STATUS_ID from flightstats.com")
    exit(1)

class RunSet(object):
    def __init__(self, departure_code : str, departure_date) -> None:
        self.departure_airport_code = departure_code
        self.departure_date = departure_date
        self.output_dir = ("%s_%s" % (args.out, departure_code))
        self.output_filename = ("%s_%s" % (departure_code,
                                              departure_date.strftime('%Y-%m-%d')))

        
        lock = RateLimiter(max_messages=60, every_seconds=60)
        self.client = FlightstatsAPI(api_key, api_id, rate_limit_lock=lock)

def get_iata_codes(csv_file_name):
    iata_codes = list()
    try:
        with open(csv_file_name, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                iata_codes.append(row['iata_codes'])
    except:
        print('Error parsing iata codes from %s' % csv_file_name)
        exit(1)

    return iata_codes

def output_flight_data(runset, flight_data, airport_data, airplane_data):
    try:
        os.mkdir(runset.output_dir)
    except FileExistsError:
        pass

    if flight_data and airport_data and airplane_data:
        # de-duplicate
        flight_data_uniq = { each['flightId'] : each for each in flight_data }.values()
        airport_data_uniq = { each['iata'] : each for each in airport_data }.values()
        airplanes_data_uniq = { each['iata'] : each for each in airplane_data }.values()

        # create panda dataframes
        flight_df = pandas.json_normalize(flight_data_uniq)
        airplane_df = pandas.json_normalize(airplanes_data_uniq)
        airport_df = pandas.json_normalize(airport_data_uniq)

        # add the departure airport info to flight data
        pdf = flight_df.merge(airport_df, left_on='departureAirportFsCode', right_on='fs')
        flight_df['depLatLon'] = pdf.agg('{0[latitude]},{0[longitude]}'.format, axis=1)
        flight_df['depAirportName'] = pdf['name']
        flight_df['depCity'] = pdf['city']
        flight_df['depCountryCode'] = pdf['countryCode']
        flight_df['depCountryName'] = pdf['countryName']
        flight_df['depRegionName'] = pdf['regionName']

        # add the arrival airport info to flight data
        pdf = flight_df.merge(airport_df, left_on='arrivalAirportFsCode', right_on='fs')
        flight_df['arrLatLon'] = pdf.agg('{0[latitude]},{0[longitude]}'.format, axis=1)
        flight_df['arrAirportName'] = pdf['name']
        flight_df['arrCity'] = pdf['city']
        flight_df['arrCountryCode'] = pdf['countryCode']
        flight_df['arrCountryName'] = pdf['countryName']
        flight_df['arrRegionName'] = pdf['regionName']

        csv_filename = ("flights_%s.csv" %
                        (os.path.basename(runset.output_filename)))
        path = os.path.join(runset.output_dir, csv_filename)
        flight_df.to_csv(path, index=False)

        csv_filename = ("airports_%s.csv" %
                        (os.path.basename(runset.output_filename)))
        path = os.path.join(runset.output_dir, csv_filename)
        pdf.to_csv(path, index=False)

        csv_filename = ("airplanes_%s.csv" %
                        (os.path.basename(runset.output_filename)))
        path = os.path.join(runset.output_dir, csv_filename)
        pdf.to_csv(path, index=False)

def run(runset):
    params = {
        'numHours' : DEFAULT_NUM_HOURS
    }

    flight_data = list()
    airport_data = list()
    airplane_data = list()
    print('Grabbing flights for %s on %s' % (runset.departure_airport_code,
                                             runset.departure_date))
    for hourOfDay in ['0','6','12','18']:
        try:
            path = PATH_TEMPLATE.format(
                departure_airport_code=runset.departure_airport_code,
                year=runset.departure_date.year,
                month=runset.departure_date.month,
                day=runset.departure_date.day,
                hourOfDay=hourOfDay
            )
            response = runset.client.call(path,
                                          numHours=params['numHours'])

            flight_data.extend(response.get('flightStatuses', None))
            appendix = response.get('appendix', None)
            if appendix:
                airport_data.extend(appendix.get('airports', None))
                airplane_data.extend(appendix.get('equipments', None))
        except FlightstatsError as e:
            return e, runset
            
    output_flight_data(runset, flight_data, airport_data, airplane_data)
            
    complete_str = ("complete: fetched %d flights" % (len(flight_data)))
    return complete_str, runset

def run_with_args():
    runset_list = list()
    departure_codes = get_iata_codes(args.departure_codes)

    iter_date = datetime.strptime(args.start_date, DEFAULT_DATE_FORMAT)
    end_date = datetime.strptime(args.end_date, DEFAULT_DATE_FORMAT)
    while iter_date <= end_date:
        for departure_code in departure_codes:
            runset_list.append(RunSet(departure_code, iter_date))
                                      
        iter_date += timedelta(days=1)

    results=[]
    if THREADED:
        results = ThreadPool(10).imap_unordered(run, runset_list)
    else:
        for runset in runset_list:
            results.append(run(runset))

    for status, runset in results:
        print("%s: %s" % (os.path.basename(runset.output_filename), status))

if __name__ == '__main__':
    #Parse arguments from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument('start_date', action='store', help='Start date for the flights')
    parser.add_argument('end_date', action='store', help='End date for the flights')
    parser.add_argument('departure_codes', action='store', help='CSV file with IATA destination codes in a column called iata_codes')
    parser.add_argument('out', action='store', help='Name of the output file')
    args = parser.parse_args()

    if args.start_date < MIN_DATE:
        print('flightstats.com only has data starting %s' % MIN_DATE)
        args.start_date = MIN_DATE

    run_with_args()
