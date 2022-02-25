# postal-code-coordinates

Developed for TrenData Texas as part of their training under UTD Capstone Project (Team 65)

This script:
  - Accesses data from Customers table from the classicmodels database
  - Cleansed data to ensure we can properly search postal codes and country codes for the GPS coordinates
  - Accessed https://pkgstore.datahub.io/ to download, parse, and store all of the ISO 3166 country codes
  - Used https://openweathermap.org/ geocoding API to get longitude/latitude
  - Created new table titled "Weather" to store CustomerNum, PostalCode, Date_Time, Latitude, Longitude, UNUSED COLUMNS FOR NOW: (Weather, Min_Temp, and Max_Temp)
  - Insert all data found via the API + data from Customers table into the new Weather table
  - Print a report of all users in the Weather table with associated PostalCode and Lat/Long

Requirements
Python 3.10

MariaDB SQL Database or Any other SQL Server Database of your choice

PIP [Optional, to install dependencies]

Install instructions:
  - Download gpsWeather.py and requirements.txt
  - Run pip install -r requirements.txt to install dependencies
  - Before running the script, change MYSQL server variables [host, user, password, database] 
  - Editable Variables are Listed on the top, under imports
  - To run the script, use $ python gpsWeather.py
