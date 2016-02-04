"""
merge the information of project_name (view file column), time, and user_name (view_project column)
in all csv files in the folder usage_history_data

and select the records in a certain period
"""
import os 
import pandas as pd
import argparse





def  mergeCSV(searchDir): 

	dfWholeFile=pd.DataFrame({'agent_name': [], "view_project": [], "Created at": []})
	count=0
	for file in os.listdir(searchDir):
		if file.endswith(".csv"):
			fullPath=searchDir+'/'+file

			df=pd.read_csv(fullPath , usecols=['view_project','agent_name', 'Created at' ])

			header=list(df.columns.values)

			userName=df['agent_name']
			timeLogin=df['Created at']
			projectName=df['view_project']
			# userName=df[header[2]]
			# projectName=df[header[4]]
			# timeLog=df[header[5]]

			###remove the records where projectName cells are blank

			##get the position of project name which is not blank 
			pos=pd.notnull(projectName)

			##filtering out the NaN values in projectName
			userName=userName[pos]
			timeLogin=timeLogin[pos]
			projectName=projectName[pos]

			# wholeUserName.append(userName)
			# wholeTimeLogin.append(timeLogin)
			# wholeProjectName.append(projectName)

			currentDf=pd.concat([userName, projectName, timeLogin], join='outer', axis=1)

			# dfWholeFile['agent_name'].append(userName)
			# dfWholeFile['view_project'].append(projectName)
			# dfWholeFile['Created at'].append(timeLogin)
			
			#dfWholeFile=pd.concat([])

			dfWholeFile=dfWholeFile.append(currentDf, ignore_index=True)
	return dfWholeFile
		# import pdb; pdb.set_trace()
		# count=count+1

		# if count==2:
		# 	import pdb; pdb.set_trace()

#get the position of records in a certain period
def truncateCSV (dfWholeFile, startDay, endDay):
	posStart=dfWholeFile['Created at']>startDay
	posEnd=dfWholeFile['Created at']<endDay

	pos=(posStart==posEnd)

	dfSelected=dfWholeFile[pos] #select the records in a certain period

	#import pdb; pdb.set_trace()

	#dfWholeFile['created']
	outputFile=open('output.csv', 'w')

	dfSelected.to_csv(outputFile)

	outputFile.close()
	
	return dfSelected
		# dfWholeFile.loc[len(df)+1]=({'userName': userName, 'projectName': projectName, 'timeLogin': timeLogin}, 
		# 					ignore_index=True)

		

		# import pdb; pdb.set_trace()
		# count=count+1

		# if count==2:
		# 	import pdb; pdb.set_trace()
if __name__=='__main__':

	# searchDir="../../suburb_profiling/usage_history_data"

	# startDay='2016-01-02'
	# endDay='2016-02-02'

	parserArg=argparse.ArgumentParser()

	parserArg.add_argument('enquiryFolder')
	parserArg.add_argument('startDate')
	parserArg.add_argument('endDate')

	args=parserArg.parse_args()

	mergedCSV=mergeCSV(args.enquiryFolder)
	truncateCSV(mergedCSV, args.startDate, args.endDate)



