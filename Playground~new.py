#HMRC workexperience project A

import os #importing os to set work directory
import polars as pl #importing polars

os.chdir("C:/Users/dazmc/OneDrive/Documents/companies-house") #setting work directory

os.getcwd() #Checking the working directory

data = pl.read_csv("BasicCompanyData-2025-04-01-part1_7.csv") #reading in the company data

compdata = pl.DataFrame(data) #setting the data as a dataframe (part1)

#slimming down part 1 of the data set
#calling it a new data set 

new1 = compdata.select(
    pl.col("CompanyName"),
    pl.col(" CompanyNumber"), #Has a space at the start
    pl.col("RegAddress.CareOf"),
    pl.col("RegAddress.POBox"),
    pl.col("RegAddress.AddressLine1"),
    pl.col(" RegAddress.AddressLine2"), #space at the start
    pl.col("RegAddress.PostTown"),
    pl.col("RegAddress.County"),
    pl.col("RegAddress.Country"),
    pl.col("RegAddress.PostCode"),
    pl.col("CompanyCategory"),
    pl.col("CompanyStatus"),
)
print(new1)
new1.shape #849999 rows (companies), 12 columns (variables)
new1.write_parquet("test.parquet")

def slimdata (data):  #A function that slims each data set down :)
    return data.select(
        pl.col("CompanyName"),
        pl.col(" CompanyNumber"), #Has a space at the start
        pl.col("RegAddress.CareOf"),
        pl.col("RegAddress.POBox"),
        pl.col("RegAddress.AddressLine1"),
        pl.col(" RegAddress.AddressLine2"), #space at the start
        pl.col("RegAddress.PostTown"),
        pl.col("RegAddress.County"),
        pl.col("RegAddress.Country"),
        pl.col("RegAddress.PostCode"),
        pl.col("CompanyCategory"),
        pl.col("CompanyStatus"),
    )

#created a function to run through each data set and slim them down 
def create_csv(file_number):
    a = pl.read_csv("BasicCompanyData-2025-04-01-part" + str(file_number) + "_7.csv")
    b = slimdata(a)
    b.write_csv("test" + str(file_number) + ".csv")

create_csv(7) #part 7

t = pl.DataFrame([]) #creating an empty dataframe
for i in range(7): #loops through all the datasets and joins them together
    i = i+1 
    x = pl.read_csv("test" + str(i) + ".csv")
    t = pl.concat([t, x], how= "vertical")

t.shape  #5,656,095 companies with 12 variables.

#extract ixbrl file

from ixbrlparse import IXBRL
with open('Prod223_3928_00043435_20241031.html', encoding="utf8") as a:
    x = IXBRL(a)

x.numeric[0].name #.name / .value / .schema / .context

#list of 4 things, name, value, schema, context

#a function that takes the numeric and non-numeric values
def getx2(x):
    z=[]
    z.append(x.name)
    z.append(x.value)
    z.append(x.schema)
    z.append(x.context)
    return z

getx2(x.numeric[0]) 
getx2(x.nonnumeric[0])

#look at maps in python 
num_fields = list(map(getx2, x.numeric))
num_fields

#A function that will take the path and return the numeric and non-numeric values as a dictonary
def pathval(path): #input path in speach marks!
    p = path #set value to path
    with open(p, encoding="utf8") as a: 
        p2 = IXBRL(a) #opens the ixbrl file
    p3 = list(map(getx2, p2.numeric)) #records the numeric values
    p4 = list(map(getx2, p2.nonnumeric)) #the numeric values
    pdict = {"numeric_values": p3,  #returns both values as dictonary.
             "non-numeric_values": p4}
    return pdict

data1 = pathval('Prod223_3928_00043435_20241031.html') #our first file 

#function that reads path and splits dictonary with crn and balance sheet date
def compdetails(path):
    p = path
    chunks = p.split('_') #splits the chunks
    relevchunk1 = chunks[2] #assigns the company number
    relevchunk12 = chunks[3].split('.') #splits further
    relevchunk2 = relevchunk12[0] #assigns balance sheet date
    chunkdict = {"CompanyNumber": relevchunk1,
                 "Balance sheet date": relevchunk2} #creates dictonary
    return (chunkdict)

data2 = compdetails('Prod223_3928_00043435_20241031.html') 


def allinfo (path):
    p = path
    d1 = pathval(p) #info of company
    d2 = compdetails(p) #CRN and balance sheet date
    combineddict = {**d1, **d2} #combining dictonaries
    return combineddict

allinfo('Prod223_3928_00043435_20241031.html')

import zipfile

def accountreader(zipfolder):
    allallinfo = [] #an empty dataframe where the data will be stored
    zip = zipfile.ZipFile(zipfolder, "r") #reads the zip file
    extra = zip.extractall() #extracts all data
    zipfiles = zip.namelist() #names of each file
    for i in range(len(zipfiles)): #to loop through the zip folder
        try:
            print(zipfiles[i])
            b = allinfo(zipfiles[i]) #returns the info
            allallinfo.append(b)
            i = i+1
        except: 
            allallinfo.append([])
            i = i+1        
    return allallinfo

#line below takes a long time to run 
accountdetails = accountreader("Accounts_Bulk_Data-2025-04-02.zip")
accountdetails[5] #returns the 6th value

import pickle

#Saves the dataset so we dont have to run accountdetails again :)
with open('AccountsData.pickle', 'wb') as handle:
    pickle.dump(accountdetails, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('AccountsData.pickle', 'rb') as handle: #loads in the dataset using pickle
    Accounts_Bulk_Data = pickle.load(handle)

Accounts_Bulk_Data[5] #also returns the 6th value

polars1 = pl.DataFrame([Accounts_Bulk_Data[5]], strict=False)
polars1 #turns into a polars dataframe

len(Accounts_Bulk_Data) #number of accounts 

#stores everything in a data set
dataframe1 = pl.DataFrame() #creates an empty dataframe
for i in range(len(Accounts_Bulk_Data)): #runs through all of account data
    try:
        polarstemp = pl.DataFrame([accountdetails[i]], strict = False)
        dataframe1 = pl.concat([dataframe1, polarstemp], how="vertical")
        i = i + 1
        print(i)
    except:
        print("bad")

print(dataframe1)

with open('dataframe1.pickle', 'wb') as handle:
    pickle.dump(dataframe1, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('dataframe1.pickle', 'rb') as handle:
    dataframe1 = pickle.load(handle)

#cleaning the original dataset
t = t.rename({" CompanyNumber": "CompanyNumber", "RegAddress.PostCode": "PostCode",
              "RegAddress.Country": "Country", "RegAddress.County": "County",
              "RegAddress.PostTown": "PostTown", " RegAddress.AddressLine2": "AddressLine2",
              "RegAddress.AddressLine1": "AddressLine1"})

tnew = t.drop("RegAddress.CareOf", "RegAddress.POBox")

#joining the dataframes together using the company number
joineddata = t.join(dataframe1, on="CompanyNumber")

testingofjoineddata = joineddata[0]

num_val = (testingofjoineddata.select(["CompanyNumber", "Balance sheet date", "numeric_values"]))
num_val = num_val.explode('numeric_values')
num_val.with_columns(pl.col("numeric_values").list.to_struct()).unnest('numeric_values')
testingofjoineddata.select(["CompanyNumber", "Balance sheet date", "numeric_values"])

#create numeric and non numeric (function) that stores the values in two dataframes one for
#numeric and one for non-numeric
def NumericAndNonNumeric(dataset):  #outputs the numeric and non-numeric values in a table
    num_val = (dataset.select(["CompanyNumber", "Balance sheet date", "numeric_values"]))
    num_val = num_val.explode('numeric_values')
    num_val = num_val.with_columns(pl.col("numeric_values").list.to_struct()).unnest('numeric_values')
    a = num_val.rename({"field_0": "Variable", "field_1": "Value", "field_2": "1", "field_3":"2"})

    nonnum_val = (dataset.select(["CompanyNumber", "Balance sheet date", "non-numeric_values"]))
    nonnum_val = nonnum_val.explode('non-numeric_values')
    nonnum_val = nonnum_val.with_columns(pl.col("non-numeric_values").list.to_struct()).unnest('non-numeric_values')
    b = nonnum_val.rename({"field_0": "Variable", "field_1": "Value", "field_2": "1", "field_3":"2"})

    return(a, b)

nandnn = NumericAndNonNumeric(joineddata) #two datasets for each company with their numeric and 
#non-numeric values

test = nandnn[0].filter(pl.col("CompanyNumber").is_in(["05884007"]))
test #returns all numeric values for company 05884007

def details (number):
    tabledata = tnew.filter(pl.col("CompanyNumber").is_in([number])) #original table
    numeric = nandnn[0].filter(pl.col("CompanyNumber").is_in([number])) #numeric
    nonnumeric = nandnn[1].filter(pl.col("CompanyNumber").is_in([number])) #non-numeric
    return (tabledata, numeric, nonnumeric)
    
details("10062449")


def details2 (excelfile, accountsfile, companynumber): #excelfile must be dataframe
    accounts = accountreader(accountsfile)
    dataframe1 = pl.DataFrame() #creates an empty dataframe
    for i in range(len(accounts)): #runs through all of account data
        try:
            polarstemp = pl.DataFrame([accounts[i]], strict = False)
            dataframe1 = pl.concat([dataframe1, polarstemp], how="vertical")
            i = i + 1
            print(i)
        except:
            print("bad")
    joineddata = excelfile.join(dataframe1, on="CompanyNumber") #joining datasets together
    nandnn = NumericAndNonNumeric(joineddata)
    tabledata = excelfile.filter(pl.col("CompanyNumber").is_in([companynumber])) #original table
    numeric = nandnn[0].filter(pl.col("CompanyNumber").is_in([companynumber])) #numeric
    nonnumeric = nandnn[1].filter(pl.col("CompanyNumber").is_in([companynumber])) #non-numeric
    return (tabledata, numeric, nonnumeric)

a = details2(tnew, "Accounts_Bulk_Data-2025-04-02.zip", "08183152")

#returns three data tables for all compamies
def companydetails3 (excelfile, accountsfile): #excelfile must be dataframe
    accounts = accountreader(accountsfile)
    dataframe1 = pl.DataFrame() #creates an empty dataframe
    for i in range(len(accounts)): #runs through all of account data
        try:
            polarstemp = pl.DataFrame([accounts[i]], strict = False)
            dataframe1 = pl.concat([dataframe1, polarstemp], how="vertical")
            i = i + 1
            print(i)
        except:
            print("bad")
    joineddata = excelfile.join(dataframe1, on="CompanyNumber") #joining datasets together
    nandnn = NumericAndNonNumeric(joineddata)
    tabledata = joineddata #original table
    numeric = nandnn[0]   #numeric
    nonnumeric = nandnn[1]  #non-numeric
    return (tabledata, numeric, nonnumeric)

attempt = companydetails3(tnew, "Accounts_Bulk_Data-2025-04-02.zip")
