import pymysql     
import requests
from urllib.request import urlopen
import pandas

# variables - change host/user/pw if needed
host = 'localhost'
user = 'root'
password = 'testnewpassword'
database = 'classicmodels'

#variables for classicmodels database
tableNameCust = 'customers'
customerNum = 'customerNumber'
customerPostalCode = 'postalCode'
customerCountry = 'country'
tableNameWeather = 'weather'

#variables for links used to get files and use APIs
url = "https://datahub.io/core/country-list/r/data.csv"
geoCode1 = "http://api.openweathermap.org/geo/1.0/zip?zip="
geoCode2 = "&appid="
openWeatherKey = "aa0247a85f925e67652dd4f439105ec0" #change as needed for whoever's API key is in use


##### CONNECT TO DB ####
print("CONNECTING TO MYSQL DATABASE\n")
connection = pymysql.connect(host=host, user=user, password=password, database=database)
cursor = connection.cursor()
print("DATABASE LINK ESTABLISHED!\n")

###### SELECT CUSTNUM, ZIP, & COUNTRY FROM CUSTOMER TABLE #####
selectZip = "SELECT " + customerNum + ", " + customerPostalCode + ", " + customerCountry + " FROM " + tableNameCust + ""
cursor.execute(selectZip)
print("SELECT CUSTNUM, ZIP, & COUNTRY FROM CUSTOMER TABLE\n")
records = cursor.fetchall() #results of statement stored into records variable
custRecords = []
custRecords = list(map(list, records))


##### DATA CLEANSING - ALTER ZIPS FOR: SWEDEN, NORWAY, BELGIUM, FINLAND: 'N ', 'N-', 'S-', 'B-', 'FIN-' #####
zipCodeIssues = ['N ', 'N-', 'S-', 'B-', 'FIN-']
tempSplit = []

for zipErrors in zipCodeIssues: #iterate through each item in zipCodeIssues list
    for issues in custRecords: #iterate through each row in select stmt
        if str(zipErrors) in str(issues[1]): #this checks if the current element in zipCodeIssues is in the custRecords PostalCode string
            tempSplit = str(issues[1]).split(str(zipErrors))
            splitString = str(tempSplit[1])
            
            #replace with corrected postal code
            del issues[1]
            issues.insert(1, splitString)
            #print("TEST REPLACED ZIP: " + str(issues[1]))


##### DOWNLOAD COUNTRY CODE .CSV FILE FROM WEBSITE #####
print("DOWNLOAD DATA FROM DATAHUB.IO\n")
countryDataFrame = pandas.read_csv(url)
countryData = countryDataFrame.values.tolist() #convert dataframe to list


###### NEED TO CORRECT UNITED STATES TO USA #####
###### NEED TO CORRECT UNITED KINGDON TO UK #####
# Data cleansing for appropriate comparision w/ ISO 3166 country code list
for listAll in countryData:
    for element in listAll:
        if element == "United States":
            del listAll[0]
            listAll.insert(0, "USA")
            #print("listAll string: " + str(listAll) + "\n")
            #print("United States replaced with USA!\n")
        elif element == "United Kingdom":
            del listAll[0]
            listAll.insert(0, "UK")
            #print("listAll string: " + str(listAll) + "\n")
            #print("United Kingdom replaced with UK!\n")
        else: 
            break
print("SUCCESSFULLY DOWNLOAD DATA FROM DATAHUB.IO\n")


#### CREATE NEW LIST HOLDING custNum, postal code, country code ####
tempData = []
newData = []
count = 0

for custRow in custRecords: #iterates through select stmt results one by one
    for i, j in countryData: #iterates through the csv results one by one
        if str(custRow[2]) == str(i):
            tempData = [str(custRow[0]), str(custRow[1]), str(custRow[2]), str(j)]
            newData.append(tempData)
            count = count + 1
            break 


##### CREATE FINAL LIST (custNum, postal code, lat, long) WITH GPS DATA AQUIRED VIA API TO STORE INTO DATABASE #####
tempWeatherData = []
newWeatherData = []

for item in newData:
    try:
        resp = requests.get(geoCode1 + item[1] + "," + item[3] + geoCode2 + openWeatherKey, timeout=5)
        jsonData = resp.json()

        #handling for missing data
        if jsonData.get('cod') == '404':
            print("Latitude/longitude data not found for customer " + str(item[0]) + " with postal code " + str(item[1]))

        tempWeatherData = [str(item[0]), str(item[1]), str(jsonData.get('lat')), str(jsonData.get('lon'))]
        newWeatherData.append(tempWeatherData)
    except requests.exceptions.HTTPError as errh:
        print(errh)
    except requests.exceptions.ConnectionError as errc:
        print(errc)
    except requests.exceptions.Timeout as errt:
        print(errt)
    except requests.exceptions.RequestException as err:
        print(err)


# DELETE PREVIOUS TABLE: USE THIS DURING TESTING ONLY. CAN REMOVE BEFORE SUBMISSION.
cursor.execute("DROP TABLE IF EXISTS " + tableNameWeather)
print("\nTABLE " + tableNameWeather + " DELETED IF EXISTS\n")


#### CREATE NEW TABLE TO STORE DATA FOUND IF NOT EXISTS ####
# plan for adding weather data for improvements
create_table = "CREATE TABLE IF NOT EXISTS " + tableNameWeather + """ ( 
    CUSTNUM INT NOT NULL,
	POSTALCODE VARCHAR(15) NOT NULL, 
    DATE_TIME DATETIME DEFAULT CURRENT_TIMESTAMP,
	LATITUDE VARCHAR(12) NOT NULL, 
	LONGITUDE VARCHAR(12) NOT NULL, 
    WEATHER VARCHAR(30) DEFAULT NULL, 
    MIN_TEMP DOUBLE(5,2) DEFAULT NULL, 
    MAX_TEMP DOUBLE(5,2) DEFAULT NULL,
	PRIMARY KEY (CUSTNUM, POSTALCODE, DATE_TIME), 
	FOREIGN KEY (CUSTNUM) REFERENCES CUSTOMERS (CUSTOMERNUMBER))"""
cursor.execute(create_table)
print("TABLE " + tableNameWeather + " CREATED\n")


##### INSERT DATA OBTAINED FROM GPS API INTO NEW TABLE #####
for entry in newWeatherData: 
    insertLatLong = "INSERT INTO " + tableNameWeather + "(CUSTNUM, POSTALCODE, LATITUDE, LONGITUDE) "
    insertLatLong += " VALUES ('" + str(entry[0]) + "', '" + str(entry[1]) + "', '" + str(entry[2]) + "', '"
    insertLatLong += str(entry[3]) + "')"
    cursor.execute(insertLatLong)
print("DATA ENTRY FINISHED WITH " + str(len(newWeatherData)) + " ENTRIES!\n")



##### REPORT FOR EACH CUSTOMER IN THE CUSTOMERS TABLE! #####  

print(" ~~~~~WEATHER TABLE ~~~~~ ")
print("CUSTNUM | POSTALCODE | DATE | LATITUDE | LONGITUDE ")
queryWeatherTable = "SELECT * FROM WEATHER"
cursor.execute(queryWeatherTable)
weatherTable = cursor.fetchall()
for eachRow in weatherTable: 
    print(str(eachRow[0]) + ", " + str(eachRow[1]) + ", " + str(eachRow[2]) + ", " + str(eachRow[3]) + ", " + str(eachRow[4]))



############## WEATHER API WORK - MUSA'S CODE #############

'''
## get latitude and longitude info from ipinfo to pass them dynamically to the weather api
latitude = str(jsonData.get('lat'))
longitude = str(jsonData.get('lon'))

## sending a get request, a response object will be returned
url = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid=00af643343e13db2203d2cc06f2ffd89'.format(latitude, longitude)
result = requests.get(url)

## check if the API call was successful
print(result.status_code)

# KARIN'S TEST PRINT
testJson = result.json()
print(testJson)

## convert the response object to JSON fomat
data = result.json()

## location data
city = data['name']
latitude = data['coord']['lat']
longitude = data['coord']['lon']

## weather data
sky_clarity = data['weather'][0]['description']
temp = data['main']['temp']
# convert temp from Kelvin to Fahrenheit
temp = format( ((temp - 273.15) * (9/5)) + 32, '0.0f')
humid = data['main']['humidity']
wind_speed = data['wind']['speed']

## print weather data
print ('City: {}'.format(city))
print ('Latitude: {}'.format(latitude))
print ('Longitude: {}'.format(longitude))
print ('Sky Clarity: {}'.format(sky_clarity))
print ('Temperature: {} degree celcius'.format(temp))
print ('Humidity: {} %'.format(humid))
print ('Wind Speed: {} m/s'.format(wind_speed))
'''

#close connection to DB
connection.commit()
connection.close()