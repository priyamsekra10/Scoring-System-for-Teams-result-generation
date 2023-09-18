import firebase_admin
from firebase_admin import credentials

cred = credentials.Certificate(r"C:\Users\priya\DATA 2\INVENTIA\Untitled Folder\sample-1-fcb7b-firebase-adminsdk-sqfvi-24fd5ad500.json")
firebase_admin.initialize_app(cred)

from firebase_admin import firestore
db = firestore.client()

# Fetch the "registrations" collection
collection_ref = db.collection('scores')

# Get all documents in the collection
docs = collection_ref.get()

# Iterate over the documents and update their data
for doc in docs:
    doc_dict = doc.to_dict()
    doc.reference.set(doc_dict)  # Update the document with the new data

    # Print the updated document data
    print(f"Document ID: {doc.id}")
    print(f"Data: {doc_dict}")




data_list = []

for doc in docs:
    doc_data = doc.to_dict()
    data_list.append(doc_data)

df = pd.DataFrame(data_list)

df






score_columns = ['score1', 'score2', 'score3', 'score4', 'score5', 'score6']
# Convert scores to int type
df[score_columns] = df[score_columns].astype(int)




df = df.drop("comment",axis = 1)
# df = df.drop("createdAt",axis = 1)
df



df['gscore'] = "0"
for i in range(len(df['teamid'])):
    df['gscore'][i] = df['score1'][i]+df['score2'][i]+df['score3'][i]+df['score4'][i]+df['score5'][i]+df['score6'][i]



    df = df.drop("score1",axis = 1)
df = df.drop("score2",axis = 1)
df = df.drop("score3",axis = 1)
df = df.drop("score4",axis = 1)
df = df.drop("score5",axis = 1)
df = df.drop("score6",axis = 1)
df



df.rename(columns={'teamEmailId': 'email_participant'}, inplace=True)
print(df)


df.rename(columns={'teamName': 'teamleadername'}, inplace=True)
df


df['count'] = df.groupby(['teamleadername', 'email'])['uid'].transform('count')
df




df = df.drop(30)

# If you want to reset the index after removing the row
df.reset_index(drop=True, inplace=True)

# Display the updated DataFrame
df



df = df.drop("count", axis = 1)
df



df['createdAt'] = pd.to_datetime(df['createdAt'])

# Sort DataFrame by 'createdAt' in descending order
df.sort_values(by=['createdAt'], ascending=False, inplace=True)

# Drop duplicate votes for the same team by the same judge
df.drop_duplicates(subset=['teamid', 'email_participant'], keep='first', inplace=True)

# Reset index
df.reset_index(drop=True, inplace=True)

df


df = df.drop("createdAt", axis = 1)
df





pivoted_df = df.pivot_table(index=['teamid', 'teamleadername', 'email_participant'], columns='email', values='gscore').reset_index()

# Rename the columns
pivoted_df.columns = ['teamid', 'teamleadername', 'email_participant'] + [col if col != 'email_participant' else '' for col in pivoted_df.columns[3:]]

# Calculate the sum of scores team id wise
sum_df = df.groupby('teamid')['gscore'].sum().reset_index()
sum_df.columns = ['teamid', 'total_score']

# Merge the pivoted_df with sum_df
result_df = pd.merge(pivoted_df, sum_df, on='teamid')
result_df


import pandas as pd

# Assuming you have already loaded your original dataframe 'df'

# Pivot the dataframe
pivoted_df = df.pivot_table(index=['teamid', 'teamleadername', 'email_participant'], columns='email', values='gscore').reset_index()

# Rename the columns
pivoted_df.columns = ['teamid', 'teamleadername', 'email_participant'] + [col if col != 'email_participant' else '' for col in pivoted_df.columns[3:]]

# Calculate the sum of scores team id wise
sum_df = df.groupby('teamid')['gscore'].sum().reset_index()
sum_df.columns = ['teamid', 'total_score']

# Calculate the count of scores team id wise
count_df = df.groupby('teamid')['gscore'].count().reset_index()
count_df.columns = ['teamid', 'judge_count']

# Calculate the out_of value (maximum possible score) for each teamid
out_of_df = df.groupby('teamid')['gscore'].apply(lambda x: len(x) * 60).reset_index()
out_of_df.columns = ['teamid', 'out_of']

# Merge the pivoted_df with sum_df, count_df, and out_of_df
result_df = pd.merge(pivoted_df, sum_df, on='teamid')
result_df = pd.merge(result_df, count_df, on='teamid')
result_df = pd.merge(result_df, out_of_df, on='teamid')

# Display the result
result_df



sorted_df = result_df.sort_values(by='total_score', ascending=False)
sorted_df.to_excel('result.xlsx')
