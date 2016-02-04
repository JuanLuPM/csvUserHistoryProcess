"""
construct the histogram of suburbs and progjects, using the dataframe constructed by mergerCsv.py
put the result into propertyPrice_database
"""
import mergeCSV 
import collections
import pandas as pd
from sqlalchemy import *

searchDir="../../suburb_profiling/usage_history_data"

startDay='2016-01-02'
endDay='2016-02-02'

wholeCSV=mergeCSV.mergeCSV(searchDir)
truncatedCSV=mergeCSV.truncateCSV (wholeCSV, startDay, endDay)

projectName=truncatedCSV['view_project']

#remove the test record
projectName=projectName[projectName.str.contains('Testing')==False]

#projectHist=projectName.value_counts()

projectHist=collections.Counter(projectName)


# #import pdb; pdb.set_trace()

projectHistDf=pd.DataFrame(projectHist.items(), columns=['projectName', 'counts'])
projectHistDf['startDate']=[startDay]*len(projectHistDf)
projectHistDf['endDate']=[endDay]*len(projectHistDf)

##construct the dataframe of the name of suburbs
suburbName=projectName.str.split('-').str[0]
suburbHist=collections.Counter(suburbName)
suburbHistDf=pd.DataFrame(suburbHist.items(), columns=['suburbName', 'counts'])



suburbHistDf['startDate']=[startDay]*len(suburbHistDf)
suburbHistDf['endDate']=[endDay]*len(suburbHistDf)



#import pdb; pdb.set_trace()

#put the results to database
engine = create_engine('mysql://root:1111@localhost:3306/propertyPrice_database', echo=False)
connection = engine.connect()

projectHistDf.to_sql(con=connection, name='project_hist', if_exists='replace', flavor='mysql', index=False)

suburbHistDf.to_sql(con=connection, name='suburb_hist', if_exists='replace', flavor='mysql', index=False)