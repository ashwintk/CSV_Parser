

# This function will take a record in and split it based on the separator and return the array.
# One must note that this function will be only called when there is no quotes character
def splitRecordAndReturnAnArray_NoDoubleQuotesCharacter(my_str, sep):
    return my_str.lstrip().strip().split(sep)

# This function will take a record in and split it based on the separator and return the array.
# One must note that this function will be only called when there is a quote character in the record
def splitRecordAndReturnAnArray_DoubleQuotesCharacter(my_str, sep, quotes_char):
    returnArr = []

    return my_str.lstrip().strip().split(sep)

def getMaxColumnFrequency(parsed_data):

    return

# This function parses a CSV file
#
#   Arguments to the function
#
#       1. File Name
#       2. Separator. Default value = , (Comma)
#       3. Skip Lines. # Lines to skip before parsing. Default value = 0
#       4. Header flag. Specifies whether a header is present or not. Default value for this flag = False
#       5. Double Quotes Character. Specifies which character to consider as the quotes character.
#          Default value - double quote
#       6. Skip Missing Values flag. Specifies whether to skip a record while it has less # columns.
#          Default value = False

import os.path

def customCSV_Parser(fileName, sep=",", skip=0, header=False, doubleQuotesChar='"', skipMissing=False):

    # Throw an error if a file does not exist
    if not os.path.exists(fileName):
        print "File name doesn't exist. Check your file path."
        exit(0)

    # Counter to store number of lines processed
    lineCounter = 0

    # Data Structure to hold the parsed CSV Data
    # Header data contains column names and metadata (data types)
    header_data = {}
    # Parsed Data - 2D Array, can be used to represent the dataset better
    parsed_data = []

    # Variable to check if header is processed
    headerProcessed = False

    # Open the file passed as an argument and create a handle for it
    with open(fileName, "r") as readHandle:
        # Iterate for every lines in the file
        for line in readHandle.readlines():
            # After skipping the number of lines specified by the user
            if lineCounter >= skip:
                # If the header flag is set & it has not been processed yet
                if header and not headerProcessed:
                    # Parse header when there is no quoting characters
                    header_data['column_names'] = splitRecordAndReturnAnArray_NoDoubleQuotesCharacter(line, sep)
                    headerProcessed = True
                else:
                    # Parse data columns - No Quoting Characters
                    record = splitRecordAndReturnAnArray_NoDoubleQuotesCharacter(line, sep)
                    # Store the data in the parsed data structure
                    i = 0 # Column count
                    temp_dict = {} # Record Structure
                    for col in record:
                        temp_dict[i] = col
                        i += 1
                    temp_dict['num_cols'] = i # To check if the number of columns match
                    parsed_data.append(temp_dict)
            lineCounter += 1
    readHandle.close()
    return


customCSV_Parser(fileName="/Users/ashwinkumar/Desktop/R_Working_Dir/FinalData.csv", skip=2, header=True)
