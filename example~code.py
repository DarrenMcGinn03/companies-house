#Functions and code for xbrl project hmrc
import os #importing os to set work directory
import polars as pl #importing polars
from ixbrlparse import IXBRL
import zipfile

os.chdir("C:/Users/dazmc/OneDrive/Documents/companies-house") #setting work directory
os.getcwd() #Checking the working directory

def slimdata (data):  #A function that slims each data set down :)
    """Sliming the data set 

    Slims our data set, selecting values from the 
    original data set so that we can combine them
    without the file being so large
    Inputs:
        The data set (basic company data part x (csv file))
    Returns:
        A slimmed version of the data set  (csv file)
    """
    return data.select(
        pl.col("CompanyName"),
        pl.col(" CompanyNumber"), 
        pl.col("RegAddress.CareOf"),
        pl.col("RegAddress.POBox"),
        pl.col("RegAddress.AddressLine1"),
        pl.col(" RegAddress.AddressLine2"),
        pl.col("RegAddress.PostTown"),
        pl.col("RegAddress.County"),
        pl.col("RegAddress.Country"),
        pl.col("RegAddress.PostCode"),
        pl.col("CompanyCategory"),
        pl.col("CompanyStatus"),
    )

def create_csv(file_number):
    """Creates a slimmed csv file

    Reads the basic company data section based
    on its file number, slims the file down using
    the slimdata function and creates a new csv file.
    Inputs:
        file number (integer between 1 and 7)
    Returns:
        slimmed csv file of the basic company data (csv file)  
    """
    a = pl.read_csv("BasicCompanyData-2025-04-01-part" + str(file_number) + "_7.csv")
    b = slimdata(a)
    b.write_csv("test" + str(file_number) + ".csv")
create_csv(1) #Do for all values

t = pl.DataFrame([]) #creating an empty dataframe
for i in range(7): #loops through all the datasets and joins them together
    i = i+1 
    x = pl.read_csv("test" + str(i) + ".csv")
    t = pl.concat([t, x], how= "vertical")

def getx2(x):
    """
    Infomation on the xbrl file

    To be used in the 'pathval' function, stores
    specific values(info) from the xbrl file into a list.
    Input:
        list from xbrl file (x.numeric/ x.nonnumeric) (list)
    Outputs:
        List of specific values (list)
    """
    z=[]
    z.append(x.name)
    z.append(x.value)
    z.append(x.schema)
    z.append(x.context)
    return z

def pathval(path): #input path in speach marks!
    """
    xbrl values from a html file

    Function that finds and stores the xbrl values
    as a dictonary (numeric and nonnumeric).
    Inputs:
        pathway (html(xbrl) file)
    Outputs:
        dictonary containing values of from the xbrl file (dictonary)
    """
    p = path #set value to path
    with open(p, encoding="utf8") as a: 
        p2 = IXBRL(a) #opens the ixbrl file
    p3 = list(map(getx2, p2.numeric)) #records the numeric values
    p4 = list(map(getx2, p2.nonnumeric)) #the numeric values
    pdict = {"numeric_values": p3,  #returns both values as dictonary.
             "non-numeric_values": p4}
    return pdict

def compdetails(path):
    """
    CRN and BSD from file name

    Extracts the company registration number
    and the balance sheet date from the file name 
    into a dictonary.
    Files are in the form of:
        "Prod223"
        "_xxxx_" A number for the specific file (No real relevance)
        "xxxxxxxx_" The company number
        "YEAR/MM/DD" The balance sheet date
    Inputs:
        html(xbrl) file (html file)
    Outputs:
        a dictonary of the CRN and the BSD (dict)
    """
    p = path
    chunks = p.split('_') #splits the chunks
    relevchunk1 = chunks[2] #assigns the company number
    relevchunk12 = chunks[3].split('.') #splits further
    relevchunk2 = relevchunk12[0] #assigns balance sheet date
    chunkdict = {" CompanyNumber": relevchunk1,
                 "Balance sheet date": relevchunk2} #creates dictonary
    return (chunkdict)

def allinfo (path):
    """
    Dictonary of combined data

    Uses previous functions, pathval and compdetails
    and combines the dictonaries so that we have a single
    dictonary containing all info on the company from
    their account data
    Inputs:
        html (xbrl) file pathway (xbrl)
    Outputs:
        dictonary of compant data (dict)
    """
    p = path
    d1 = pathval(p) #info of company
    d2 = compdetails(p) #CRN and balance sheet date
    combineddict = {**d1, **d2} #combining dictonaries
    return combineddict

def accountreader(zipfolder):
    """
    Reads all the xbrl files in the zip folder

    Extracts all the html files from the accounts zip 
    folder, reads the html files as a dictonary and stores 
    them in a list.
    Inputs:
        zip file containing xbrl files(zip)
    Outputs:
        A list containing all dictonaries of info
        from the xbrl files (list)
    """
    allallinfo = [] #an empty dataframe where the data will be stored
    zip = zipfile.ZipFile(zipfolder, "r") #reads the zip file
    extra = zip.extractall() #extracts all data
    zipfiles = zip.namelist() #names of each file
    for i in range(len(zipfiles)): #to loop through the zip folder
        try:
            i = i+1
            print(zipfiles[i])
            b = allinfo(zipfiles[i]) #returns the info
            allallinfo.append(b)
        except: 
            allallinfo.append([])            
    return allallinfo

accountdetails = accountreader("Accounts_Bulk_Data-2025-04-02.zip")

import pickle  #saves the data set
with open('AccountsData.pickle', 'wb') as handle:
    pickle.dump(accountdetails, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('AccountsData.pickle', 'rb') as handle:
    Accounts_Bulk_Data = pickle.load(handle)

dataframe1 = pl.DataFrame() #creates an empty dataframe
for i in range(len(accountdetails)): #runs through all of account data
    try:
        polarstemp = pl.DataFrame([accountdetails[i]], strict = False)
        dataframe1 = pl.concat([dataframe1, polarstemp], how="vertical")
        i = i + 1
        print(i)
    except:
        print("bad")

with open('dataframe1.pickle', 'wb') as handle:
    pickle.dump(dataframe1, handle, protocol=pickle.HIGHEST_PROTOCOL)

with open('dataframe1.pickle', 'rb') as handle:
    dataframe1 = pickle.load(handle)

joineddata = t.join(dataframe1, on=" CompanyNumber") #joins the two datasets

def NumericAndNonNumeric(dataset):  #outputs the numeric and non-numeric values in a table
    """
    Numeric and non-numeric values in a table

    After entering a dataframe to this function, it will return 
    two tables thats the numeric and non-numeric info on each company
    Inputs:
        Company dataset (dataframe)
    Outputs:
        Table of numeric info and table of non-numeric info (dataframes)
    """
    num_val = (dataset.select([" CompanyNumber", "Balance sheet date", "numeric_values"]))
    num_val = num_val.explode('numeric_values')
    num_val = num_val.with_columns(pl.col("numeric_values").list.to_struct()).unnest('numeric_values')
    a = num_val.rename({" CompanyNumber": "CompanyNumer", "field_0": "Variable", "field_1": "Value", "field_2": "1", "field_3":"2"})

    nonnum_val = (dataset.select([" CompanyNumber", "Balance sheet date", "non-numeric_values"]))
    nonnum_val = nonnum_val.explode('non-numeric_values')
    nonnum_val = nonnum_val.with_columns(pl.col("non-numeric_values").list.to_struct()).unnest('non-numeric_values')
    b = nonnum_val.rename({" CompanyNumber": "CompanyNumer", "field_0": "Variable", "field_1": "Value", "field_2": "1", "field_3":"2"})

    return(a, b)

#Renaming and cleaning the company data table
t = t.rename({" CompanyNumber": "CompanyNumber", "RegAddress.PostCode": "PostCode",
              "RegAddress.Country": "Country", "RegAddress.County": "County",
              "RegAddress.PostTown": "PostTown", " RegAddress.AddressLine2": "AddressLine2",
              "RegAddress.AddressLine1": "AddressLine1"})
tnew = t.drop("RegAddress.CareOf", "RegAddress.POBox")

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
