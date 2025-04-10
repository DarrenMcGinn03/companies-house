#Functions and code for xbrl project hmrc
import os #importing os to set work directory
import polars as pl #importing polars
from ixbrlparse import IXBRL
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



