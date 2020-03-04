"""
Flightstats API. Usage:

api = FlightstatsAPI()
r = api.call(path, params)
"""

from apiclient import APIClient
from urllib.parse import urlencode

class FlightstatsError(Exception):
    def __init__(self, code, message, response=None):
        self.code = code
        self.message = message
        self.response = response

    def __str__(self):
        return "%s (%s)" % (self.message, self.code)

    def __repr__(self):
        return "%s (%s)" % (self.__class__.__name__, self.code)


class FlightstatsAPI(APIClient):
    BASE_URL = 'https://api.flightstats.com/flex/flightstatus/historical/rest/v3/json/airport/status/'
    API_KEY_PARAM = 'appKey'
    API_ID_PARAM = 'appId'

    def __init__(self, api_key, api_id, *args, **kw):
        super(FlightstatsAPI, self).__init__(*args, **kw)
        self.api_key = api_key
        self.api_id = api_id

    def _compose_url(self, path, params=None):
        p = {self.API_ID_PARAM: self.api_id,
             self.API_KEY_PARAM: self.api_key}

        if params:
            p.update(params)

        return self.BASE_URL + path + '?' + urlencode(p)

    def _handle_response(self, response):
        r = super(FlightstatsAPI, self)._handle_response(response)

        has_error = r.get('error')
        if not has_error:
            return r

        code = str(has_error.get('httpStatusCode',''))
        message = has_error.get('errorMessage', has_error.get('errorId', ''))

        raise FlightstatsError(code, message, response=response)

    def call(self, path, **params):
        return self._request('GET', path, params=params)
    
if __name__ == '__main__':    
    import json
    import os
    
    # env variables from flightstats.com
    api_key = os.getenv('FLIGHT_STATS_KEY')
    api_id = os.getenv('FLIGHT_STATS_ID')

    if not api_key or not api_id:
        print("Missing environment variables")
        print("Provide FLIGHT_STATS_KEY and FLIGHT_STATUS_ID from flightstats.com")
        exit(1)

    api = FlightstatsAPI(api_key, api_id)
    PATH_TEMPLATE = '{departure_airport_code}/dep/{year}/{month}/{day}/{hourOfDay}'

    params = {
        'numHours' : 6
    }

    flight_data = list()
    airport_data = list()
    airplane_data = list()
    for hourOfDay in ['0','6','12','18']:
        departure_airport_code = 'WUH'
        year = '2020'
        month = '02'
        day = '01'
        path = PATH_TEMPLATE.format(
            departure_airport_code=departure_airport_code,
            year=year,
            month=month,
            day=day,
            hourOfDay=hourOfDay
        )

        try:
            response = api.call(path, numHours=params['numHours'])
            
            flight_data.extend(response.get('flightStatuses', None))
            appendix = response.get('appendix', None)
            if appendix:
                airport_data.extend(appendix.get('airports', None))
                airplane_data.extend(appendix.get('equipments', None))
        except FlightstatsError as e:
            print(e)
            break

    if not flight_data:
        # API Call failed, load from file
        resp_file = open('../example_output/response_ext.json', 'r')
        response = json.load(resp_file)

        flight_data = response.get('flightStatuses', None)
        appendix = response.get('appendix', None)
        if appendix:
                airport_data = appendix.get('airports', None)
                airplane_data = appendix.get('equipments', None)

    import pandas
    if flight_data and airport_data and airplane_data:
        # de-duplicate
        flight_data_uniq = { each['flightId'] : each for each in flight_data }.values()
        airport_data_uniq = { each['iata'] : each for each in airport_data }.values()
        airplanes_data_uniq = { each['iata'] : each for each in airplane_data }.values()
        
        flight_df = pandas.json_normalize(flight_data_uniq)
        airplane_df = pandas.json_normalize(airplanes_data_uniq)
        airport_df = pandas.json_normalize(airport_data_uniq)
        
        # add the departure airport info to flight data
        pdf = flight_df.merge(airport_df, left_on='departureAirportFsCode', right_on='fs')
        flight_df['depLatitue'] = pdf['latitude']
        flight_df['depLongitude'] = pdf['longitude']
        flight_df['depAirportName'] = pdf['name']
        flight_df['depCity'] = pdf['city']
        flight_df['depCountryCode'] = pdf['countryCode']
        flight_df['depCountryName'] = pdf['countryName']
        flight_df['depRegionName'] = pdf['regionName']

        # add the arrival lat,lon column to flight data
        pdf = flight_df.merge(airport_df, left_on='arrivalAirportFsCode', right_on='fs')
        flight_df['arrLatitue'] = pdf['latitude']
        flight_df['arrLongitude'] = pdf['longitude']
        flight_df['arrAirportName'] = pdf['name']
        flight_df['arrCity'] = pdf['city']
        flight_df['arrCountryCode'] = pdf['countryCode']
        flight_df['arrCountryName'] = pdf['countryName']
        flight_df['arrRegionName'] = pdf['regionName']

        flight_df.to_csv("flights.csv", index=False) # drops the index column
