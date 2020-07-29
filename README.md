# UpdateAggregateCSV

Over the course of ODK Data collection, there has been a need to preload previously collected data into ODK. 
Preloading data to ODK has advantages like; make data entry easier, shorten interview time and improve data quality.

I have helped develop a python program to help in automatically updating the csv data file directly from the database so that the interviewer just
downloads new form and the accompanying updated data.

Hope it helps!

# Usage

after installing the package:

from DataUpPackage.main import DataUp as du
connection = du.connect("dbuser","dbpass","hostname/ip","dbport","dbname")
du.UpdateData("filename.csv","formid","database_schema",connection)

Dont hesitate to share your feedback at Qollinsochieng@gmail.com
Thank you!
