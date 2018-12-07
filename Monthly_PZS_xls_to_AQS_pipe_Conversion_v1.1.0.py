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
                                2. GUI
---------------------------------------------------------------------------'''

'''------------------
a) get filename of input antelope island data file'''
    

#main tkinter window=msater
master = Tk()


master.attributes("-topmost", True)      #makes the dialog appear on top


'''------------------
b) create some lists for the buttons'''
    
dates=[x for x in np.arange(1,32)]      #need days of the month
years=[x for x in np.arange(2016,2030)] #need years
mon_dict={'January':1,'February':2, 'March':3,'April':4, 'May':5,
               'June':6, 'July':7, 'August':8, 'Spetember':9, 'October':10, 
               'November':11, 'December':12}
mon__len_dict={'January':31,'February':28, 'March':31,'April':30, 'May':31,
               'June':30, 'July':31, 'August':31, 'September':30, 'October':31, 
               'November':30, 'December':31}
'''------------------
c) create buttons and dropdowns   
  -each w is a tk object, just got w from the code I copied from the internet
  -each variable is a field in the tk windown. variables1, 2, and 3 are the 
   fields for the start date: Month, day, year respectively
  -I used the grid method to indicate the placement of each button, as it is
   more precise than pack
    ''' 
#start date month
variable1 = StringVar(master)
variable1.set("January") # default value
w = OptionMenu(master, variable1, 'January','February', 'March','April', 'May',
               'June', 'July', 'August', 'September', 'October', 'November', 
               'December')
w.grid(row=1, sticky=W, column=0)

#start date day of month
variable2 = StringVar(master)
variable2.set(1) # default value
w2 = OptionMenu(master, variable2, *dates)
w2.grid(row=1, sticky=W, column=1)

#start date year
variable3 = StringVar(master)
variable3.set(2019) # default value
w3 = OptionMenu(master, variable3, *years)
w3.grid(row=1, sticky=W, column=2)

#end date month
variable4 = StringVar(master)
variable4.set('January') # default value
w4 = OptionMenu(master, variable4, 'January','February', 'March','April', 'May',
               'June', 'July', 'August', 'September', 'October', 'November', 
               'December')
w4.grid(row=3, sticky=W, column=0)

#end date day of month
variable5 = StringVar(master)
variable5.set(31) # default value
w5 = OptionMenu(master, variable5, *dates)
w5.grid(row=3, sticky=W, column=1)

#end date year
variable6 = StringVar(master)
variable6.set(2019) # default value
w6 = OptionMenu(master, variable6, *years)
w6.grid(row=3, sticky=W, column=2)

'''------------------
d) get data from GUI'''

def auto_get():
    variable4.set(variable1.get())
    variable5.set(mon__len_dict.get(variable1.get()))
    variable6.set(variable3.get()) # default value
    
def quit():
    global cut_date1
    global cut_date2
    cut_date1=str(mon_dict.get(variable1.get())) +'-'+ str(variable2.get()) +'-'+ str(variable3.get())
    cut_date2= str(mon_dict.get(variable4.get())) +'-'+ str(variable5.get()) +'-'+ str(variable6.get())
    master.destroy()

Autoset = Button(master, text="Auto \n End Date", command=auto_get)
Autoset.grid(row=5, sticky=W, column=0)

button = Button(master, text="OK", command=quit)
button.grid(row=5, sticky=W, column=2)

labelText=StringVar()
labelText.set("Choose State Date")
label1=Label(master, textvariable=labelText, height=4)
label1.grid(row=0, sticky=W, column=1, columnspan=2)

labelText=StringVar()
labelText.set("Choose End Date")
label1=Label(master, textvariable=labelText, height=4)
label1.grid(row=2, sticky=W, column=1)


mainloop()





'''--------------------------------------------------------------------------
         2. Create AQS Output Dataframe and Check for 2 Week Gaps
                         
This section focuses on the pandas df 'output_df'. I use this df to store up
all the info needed for an AQS upload that can be easily saved to a pipe 
delimited csv.
----------------------------------------------------------------------------'''



#directory=audit_path()
directory='U:/PLAN/BCUBRICH/PZS/2018/November'


'''-----initialize some variables and dictionaries-----'''


gap = pd.Timedelta('14 days')
output_df=pd.DataFrame()
gap_count=0
converters={'PerformingAgencyCode':str,'State':str,
            'County':str, 'Site':str, 'ParamterCode':str, 
            'POC':str, 'MethodCode':str, 'ReportingUnit':str,
            'ParameterCode':str, 'AssessmentDate':str}
prec_dict={'42101':10, '44201':7, '42401':10,'42601':15,
           '42602':15,'42600':15,'42603':15, '42612':15}
span_dict={'42101':10, '44201':7, '42401':10,'42601':10,
           '42602':10, '42600':10, '42603':10, '42612':10}
param_dict={'42101':'CO', '44201':'O3', '42401':'SO2','42601':'NO',
           '42602':'NO2', '42600':'NOy', '42603':'NOx', '42612':'NOy-NO Diff'}

print('\n\n')
print('--------checking for 2 week time gaps---------------')
print('\n')


for filename in os.listdir(directory):     #loop through files in user's dir
    if filename.lower().endswith('.xls') or filename.lower().endswith('.xlsx'):
        xls_df=pd.read_excel((directory + '\\' + filename), n_rows=4, usecols="A:V", converters=converters)
        xls_df['date_normal']=xls_df['AssessmentDate'].str[4:6]+'-'+xls_df['AssessmentDate'].str[-2:]+'-'+xls_df['AssessmentDate'].str[:4]
        xls_df['datetime']=pd.to_datetime(xls_df['date_normal'])
        station = "{}-{}-{}".format(xls_df.loc[0,'State'], xls_df.loc[0,'County'], xls_df.loc[0,'Site'])
        for param in xls_df['ParameterCode'].unique():
            
            
            PZS_date=xls_df[xls_df['ParameterCode']==param].AssessmentDate.sort_values().copy()
            for date1, date2 in zip(PZS_date, PZS_date[1:]):
                date1=date1
                date2=date2
                date1=date1[4:6]+'-'+date1[-2:]+'-'+date1[:4]
                date2=date2[4:6]+'-'+date2[-2:]+'-'+date2[:4]
                delta=pd.to_datetime(date2)-pd.to_datetime(date1)
#                print (delta)
                if delta>gap:
                    print('At station {} the parameter {} ({}) has a gap \
                          greater than 14 days between {} and {}. Null \
                          hourly data between these dates. Data are contained\
                          in the file {}'.format(station, param, param_dict.get(param), date1, date2, filename))
                    print('\n')
                    gap_count+=1
        xls_df['Filename']=filename
        output_df=output_df.append(xls_df)
if gap_count>0:
    print ('WARNING!!!!!!!!')
    print ("Some stations contain gaps greater than 14 days, be sure to null the appropriate data")
if gap_count==0:
    print ("No PZS gaps greater than 2 weeks in any files!!")
print('\n')

'''----------------------------------------------------------------------------
                             3.  Check Prec Span
---------------------------------------------------------------------------'''


print('-----------checking PZS values....----------------------')
print('\n')


output_df=output_df.fillna(value='0')
output_df['MaxPZS']=[max(x) for x in zip(output_df['Low7Test'].str.split('%').str[0].apply(float),
          output_df['Low10Test'].str.split('%').str[0].apply(float),
          output_df['Low15Test'].str.split('%').str[0].apply(float), 
          output_df['High7Test'].str.split('%').str[0].apply(float),
          output_df['High10Test'].str.split('%').str[0].apply(float),
          output_df['High15Test'].str.split('%').str[0].apply(float), )]



max_pzs=[]
for param_code, ps in zip(output_df['ParameterCode'],output_df['PhaseName']):
    if 'prec' in ps.lower() or 'pres' in ps.lower():
        max_pzs.append(prec_dict.get(param_code))
    elif 'span' in ps.lower():
        max_pzs.append(span_dict.get(param_code))
    else: 
        max_pzs.append('error!!!')

output_df['PZS_diff']=(output_df['Value']-output_df['ExpectedValue'])/output_df['ExpectedValue']*100
output_df['MaxAllowed']=max_pzs
output_df['PZS_Check']=np.where(np.abs(output_df['PZS_diff'])>=output_df['MaxAllowed'],'FAIL!!!','PASS')

pzs_fail_df=output_df[output_df['PZS_Check']=='FAIL!!!'].copy()
    #        print(PZS_test)


#Ne we wa gonna print out all the failed entries so that can be further QCed
print('The following entries failed the PZS checks:')
for state, county, site, parameter, date, file in zip(pzs_fail_df['State'], 
                                                pzs_fail_df['County'], 
                                                pzs_fail_df['Site'], 
                                                pzs_fail_df['ParameterCode'],
                                                pzs_fail_df['date_normal'],
                                                pzs_fail_df['Filename']):
    print('{} ({}) at site {}-{}-{} failed PZS on {} (Data from file {})'.format(parameter,param_dict.get(parameter), state, county, site, date, file))

'''----------------------------------------------------------------------------
                             4.  Output Cleanup
---------------------------------------------------------------------------'''


#Drop the data back down to the reporting month, data currently has a 2 week buffer
#on either side to make sure that the PZSs have been run frequently enough.
output_df=output_df[output_df['datetime']>=pd.to_datetime(cut_date1)]
output_df=output_df[output_df['datetime']<=pd.to_datetime(cut_date2)]


columns_to_keep=output_df.columns[:15] #only need these columns for the AQS upload
output_df=output_df[columns_to_keep] 
'''       -------------Duplicate Check-------------------------------       
AQS won't accept 2 entries for the same instrument at the same station on the 
same day, so we need to remove any duplicates like that.'''

subset=output_df.columns[:10] #subset of df columns to perform duplicate check on
output_df=output_df.sort_values(['State', 'County', 'Site', 'ParameterCode',
                                 'AssessmentDate']).drop_duplicates(subset =subset, keep ='last') #drop duplicates, take second entry

'''----------------------------------------------------------------------------
                             5.  Write to file
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
