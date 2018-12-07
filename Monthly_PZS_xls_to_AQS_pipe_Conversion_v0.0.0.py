#%%
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 18:06:04 2018

@author: bcubrich


SUMMARY
--------------------------------
This code takes audit files for gaseous data and collects the audit and 
indicated measurements, then outputs a pipe delimited text file called
 'QA_output.txt' that can be directly uploaded to AQS.

INDEX
-------------------------------
1. Functions
    -functions to get filenames and directories    

2. Create Output Dataframe
    -Take the data from the user input PZS excel files forms and convert it to a pandas
    data frame (df). Create a df that has all of the data from all of the stations 
    for the month which is being reported for. Check for 2 weeks gaps in PZS
    so that data can be nulled where appropriate.

3. Write to file 
    -Write the above df to a file



"""




import pandas as pd
import numpy as np         #didn't even use numpy!!! HA!
#import seaborn as sns
from tkinter import Tk
from tkinter.filedialog import askopenfilename
#import matplotlib.pyplot as plt
import os
#import xlrd
import wx

'''---------------------------------------------------------------------------
                                1. Functions
----------------------------------------------------------------------------'''

#The following functions are just used to get filepaths
#I usually just run it once to get the path, and then leave this 
#fucntion so that I can get othe rpaths if needed
def get_dat():
    root = Tk()
    root.withdraw()
    root.focus_force()
    root.attributes("-topmost", True)      #makes the dialog appear on top
    filename = askopenfilename()      # Open single file
    root.destroy()
    return filename

#filename=get_dat()

#Thanks Kristy Weber 11/15/18 for giving me these functions to specify directories
def audit_path():
    app = wx.App()
     
    frame = wx.Frame(None, -1, 'win.py')
    frame.SetSize(0,0,200,50)
     
    # Create open file dialog
    openFileDialog = wx.DirDialog(frame, "Choose directory with Monthly PZS data", "",
                wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
     
    openFileDialog.ShowModal()
    print(openFileDialog.GetPath())
    
    # outfile_path is the string with the path name saved as a variable
    path = openFileDialog.GetPath()#+'\\'
    openFileDialog.Destroy()
    
    del app
    return path
'''--------------------------------------------------------------------------
         2. Create AQS Output Dataframe and Check for 2 Week Gaps
                         
This section focuses on the pandas df 'output_df'. I use this df to store up
all the info needed for an AQS upload that can be easily saved to a pipe 
delimited csv.
----------------------------------------------------------------------------'''



#directory=audit_path()
directory='U:/PLAN/BCUBRICH/PZS/2018/November'

gap = pd.Timedelta('14 days')
output_df=pd.DataFrame()
gap_count=0

for filename in os.listdir(directory):     #loop through files in user's dir
    
    xls_df=pd.read_excel((directory + '\\' + filename), n_rows=4, usecols="A:O")
    station = "{}-{}-{}".format(xls_df.loc[0,'State'], xls_df.loc[0,'County'], xls_df.loc[0,'Site'])
    for param in xls_df['ParameterCode'].unique():
        PZS_test=xls_df[xls_df['ParameterCode']==param].AssessmentDate.copy()
        for date1, date2 in zip(PZS_test, PZS_test[1:]):
            date1=str(date1)
            date2=str(date2)
            date1=date1[4:6]+'-'+date1[-2:]+'-'+date1[:4]
            date2=date2[4:6]+'-'+date2[-2:]+'-'+date2[:4]
            delta=pd.to_datetime(date2)-pd.to_datetime(date1)
            if delta>gap:
                print('At station {} the paramter {} has gap greater than 14 days between {} and {}. Null hourly data between these dates.'.format(station, param, date1, date2))
                gap_count+=1
    
    output_df=output_df.append(xls_df)
    if gap_count>0:
        print ("Some stations contain gaps greater than 14 days, be sure to null the appropriate data")
                
#        print(PZS_test)


'''----------------------------------------------------------------------------
                             3.  Write to file
---------------------------------------------------------------------------'''



out_path =directory #get user selected output path


output_df=output_df.set_index('TransactionType') #need to get rid of index
output_df.to_csv(out_path+'\Monthly_PZS.txt', sep='|')    #write to pipe file


'''---------
The following whole bit is used to add a '#' to the first line of the file. 
Seems like a lot of code just to add a hashtag to the file, but I like having 
the header info right in the file, in case someone only sees the text file.
------'''
appendText='#'
text_file=open(out_path+'\Monthly_PZS.txt','r')
text=text_file.read()
text_file.close()
text_file=open(out_path+'\Monthly_PZS.txt','w')
text_file.seek(0,0)
text_file.write(appendText+text)
text_file.close()
