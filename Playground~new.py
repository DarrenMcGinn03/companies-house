#HMRC workexperience project A

import os #importing os to set work directory
import polars as pl #importing polars

os.chdir("C:/Users/dazmc/OneDrive/Documents/companies-house") #setting work directory

os.getcwd() #Checking the working directory

data = pl.read_csv("BasicCompanyData-2025-04-01-part1_7.csv") #reading in the company data

compdata = pl.DataFrame(data) #setting the data as a dataframe

print(compdata)

groupofcountry = compdata.group_by(
    (pl.col("CountryOfOrigin")).alias("Country Of Origin"),
    maintain_order=True, #maintain order will change the order of which grouped.
).len()

print(groupofcountry)
print(groupofcountry.shape) #147 different countries

#Country of origin is mixed
#Using Rstudio to look at the data set

#Categories of company type
catofcomp = compdata.group_by(
    (pl.col("CompanyCategory")).alias("Category of a company"),
    maintain_order=True, #maintain order will change the order of which grouped.
).len()

print(catofcomp)
print(catofcomp.shape)


#Categories of company status
statofcomp = compdata.group_by(
    (pl.col("CompanyStatus")).alias("Status of a company"),
    maintain_order=True, #maintain order will change the order of which grouped.
).len()

print(statofcomp)
print(statofcomp.shape)

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

#Loading in part 2 of the data

compdata2 = pl.read_csv("BasicCompanyData-2025-04-01-part2_7.csv") #part 2 of 7

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

new2 = slimdata(compdata2) #New data set using our defined function
new2.shape #returns the shape of the new data set (slimmed version)

new1and2 = pl.concat([new1, new2], how="vertical") #joining datasets 1 and 2 verticaly
new1and2.shape

#created a function to run through each data set and slim them down 
def create_csv(file_number):
    a = pl.read_csv("BasicCompanyData-2025-04-01-part" + str(file_number) + "_7.csv")
    b = slimdata(a)
    b.write_csv("test" + str(file_number) + ".csv")

create_csv(7) #ran for 1 through to 7. turning all slim data sets into a parquet file

t1 = pl.read_csv("test1.csv") #base 

t = pl.DataFrame([]) #creating an empty dataframe
t.shape

for i in range(7): #loops through all the datasets and joins them together
    i = i+1 
    x = pl.read_csv("test" + str(i) + ".csv")
    t = pl.concat([t, x], how= "vertical")

t.shape  #5,656,095 companies with 12 variables.

#take a sample of the xbrl docs
#dont know which part the company is in
#use the company number 
#find a package so that i can load 
#A package that will hopefully convert the xbrl document into a dataframe 
#hoping that it will convert to a dataframe 

#attempt1: python-xbrl

#pip install python-xbrl
#pip install beautifulsoup4
#pip install marshmallow==1.1.0
#pip install six

from xbrl import XBRLParser, GAAP, GAAPSerializer


xbrl_parser = XBRLParser()

xbrl = xbrl_parser.parse(file("Prod223_3928_00043435_20241031.html"))

#attempt 3

#pip install brel-xbrl

from brel import Filing

filing1 = Filing.open("Prod223_3928_00043435_20241031.html")

#attempt 4
#pip install stream-read-xbrl

import httpx
import pandas as pd
from stream_read_xbrl import stream_read_xbrl_zip
url = 'http://download.companieshouse.gov.uk/Accounts_Bulk_Data-2025-04-02.zip'
with httpx.stream('GET', url) as r, stream_read_xbrl_zip(r.iter_bytes(chunk_size=65536)) as (columns, rows):
    df = pd.DataFrame(rows, columns=columns)

print(df)

#extract ixbrl file
#company number and date in name of file??

from ixbrlparse import IXBRL
with open('Prod223_3928_00043435_20241031.html', encoding="utf8") as a:
    x = IXBRL(a)

print(x.contexts)

print(x.filetype)

print(x.units)

print(x.numeric)

type(x.numeric)

x.numeric[0].schema

#list of 4 things, name, value, schema, context
#function to take values and store in a list.
#make a function of this value takes the values and stores in a table

def getx(x, a):
    z = []
    z.append(x.numeric[a].name)
    z.append(x.numeric[a].value)
    z.append(x.numeric[a].schema)
    z.append(x.numeric[a].context)
    return z

getx(x, 0) #list of values 

#a function that takes the numeric value

def getx2(x):
    z=[]
    z.append(x.name)
    z.append(x.value)
    z.append(x.schema)
    z.append(x.context)
    return z

getx2(x.numeric[1]) 
getx2(x.nonnumeric[0])

xlist = pl.DataFrame(x.numeric)
lsize = xlist.shape #13 lines

llength = lsize[0] + 1 #assigning the length
llength
x.numeric

#look at maps in python 

num_fields = list(map(lambda y: getx(x, y), range(len(x.numeric))))

num_fields = list(map(getx2, x.numeric))
num_fields

#we want a function that will take the path and return the numeric and non-numeric values
#as a dictonary

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
    
data1

#function that reads path and spits dictonary with crn compant number and balance date then combine with date
# then combine dictonary

def compdetails(path):
    p = path
    chunks = p.split('_') #splits the chunks
    relevchunk1 = chunks[2] #assigns the company number
    relevchunk12 = chunks[3].split('.') #splits further
    relevchunk2 = relevchunk12[0] #assigns balance sheet date
    chunkdict = {"CRN:": relevchunk1,
                 "Balance sheet date": relevchunk2} #creates dictonary
    return (chunkdict)

data2 =compdetails('Prod223_3928_00043435_20241031.html') 
data2


ex = 'Prod223_3928_00043435_20241031.html'
ex1 = ex.split('_')
ex1[2]
ex2 = ex1[3].split('.')
ex2[0]

data3 = {**data1, **data2}
print(data3)

def allinfo (path):
    p = path
    d1 = pathval(p) #info of company
    d2 = compdetails(p) #CRN and balance sheet date
    combineddict = {**d1, **d2} #combining dictonaries
    return combineddict

allinfo('Prod223_3928_00043435_20241031.html')



