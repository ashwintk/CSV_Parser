import os.path


# This function will take a record in and split it based on the separator and return the array.
# One must note that this function will be only called when there is no quotes character
def splitRecordAndReturnAnArray_NoDoubleQuotesCharacter(my_str, sep):
    return my_str.lstrip().strip().split(sep)


# This function will take a record in and split it based on the separator and return the array.
# One must note that this function will be only called when there is a quote character in the record
def splitRecordAndReturnAnArray_DoubleQuotesCharacter(my_str, sep, quotes_char):
    returnArr = []
    return my_str.lstrip().strip().split(sep)


# This function is used to identify the frequently occurring number of columns in the dataset
# This function is to be used when there is no header field specified
# This function takes in the parsed dataset as an argument and returns two values
#      1. All equal flag --> Denotes whether all records have the same number of columns. If they do this flag is true
#      2. Frequently occurring # columns in the dataset


def getMaxColumnFrequency(parsed_data):
    # Data Structure to store counts of columns
    count_data = {}
    # Iterate for every parsed record
    for data in parsed_data:
        # If the number of column exists already in the dictionary increase its value by one
        if data['num_cols'] in count_data.keys():
            count_data[data['num_cols']] += 1
        else:
            # If the # columns is not found in the dictionary set it to one
            count_data[data['num_cols']] = 1
    # Flag to ensure that all the records have same number of column
    all_equal_flag = True if len(count_data.keys()) == 1 else False

    if all_equal_flag:
        # If all the records have the same number of columns return true & the number of columns
        return all_equal_flag, count_data.keys()[0]
    else:
        max_count_num_rows = -1
        # Sort the dictionaly in descending order
        for k in  sorted(count_data, key=lambda k: (-count_data[k], k)):
            max_count_num_rows = k
            break
        # Return False & the max. number of columns occurring in the dataset
        return all_equal_flag, max_count_num_rows


# This function is used to filter records having frequently occurring number of columns
# This function is to be used when there is no header field specified
# This function takes in the parsed dataset as an argument and the frequently occurring number of columns
# This function returns parsed dataset


def filterFrequentlyOccurringColumns(parsed_data, frequently_occurring_columns):
    # Data Structure to store filtered dataset
    filtered_dataset = []
    # Iterate over all records in the parsed dataset
    for record in parsed_data:
        # If the number of columns is equal to the frequently occurring columns add it to filtered dataset
        if record['num_cols'] == frequently_occurring_columns:
            filtered_dataset.append(record)
    return filtered_dataset


# This function is used to return formatted data
# This function can be used when there is a header or not
# This function takes the following arguments
#      1. Parsed dataset
#      2. Header flag
#      3. Header data dictionary
# This function returns formatted dataset


def formatDataset_InferColumnDataType(parsed_data, header, freq_occuring_num=0, header_data = {}):
    # If there is no header create column names. Column names will be numbered from 1
    if not header:
        header_data['column_names'] = map(str, range(0, freq_occuring_num))
    col_count = len(header_data['column_names'])
    returned_data =[[] for _ in range(0, col_count)]

    for record in parsed_data:
        for i in range(0, col_count):
            returned_data[i].append(record[i])

    return header_data, returned_data


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

def customCSV_Parser(fileName, sep=",", skip=0, header=False, doubleQuotesChar='"', skipMissing=False):

    # Throw an error if a file does not exist
    if not os.path.exists(fileName):
        print "File name doesn't exist. Check your file path."
        exit(1)

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
                    temp_dict['num_cols'] = i  # To check if the number of columns match
                    # If the header option is enabled
                    if header:
                        # Number of columns in the header matched with this record
                        if i == len(header_data['column_names']):
                            # Add data to parsed information
                            parsed_data.append(temp_dict)
                        # If the header option is enabled and the number of columns in the dataset does'nt match
                        # with this record & line skip option is enabled
                        elif i != len(header_data['column_names']) and skipMissing:
                            # Print a message and do nothing
                            print "Line #", lineCounter, " does not confirm to the header specified. " \
                                                         "Line skip is enabled, this line is ignored"
                        # If the header option is enabled and the number of columns in the dataset does'nt match
                        # with this record & line skip option is disabled
                        else:
                            # Print a message and exit
                            print "Line #", lineCounter, " does not confirm to the header specified. " \
                                                         "Line skip is disabled. Parsing failed"
                            exit(1)
                    else:
                        # If header is not specified we do not know how many columns are there in a dataset.
                        # So I have not checked the number of columns here. To enable me to check the number of columns
                        # I have added a variable called num_cols
                        parsed_data.append(temp_dict)
            lineCounter += 1
    readHandle.close()
    freq_key = -1
    # If header is not set, check for the number of columns
    if not header:
        # Check if the number of columns in all records are same
        all_equal_flag, freq_key = getMaxColumnFrequency(parsed_data)
        if not all_equal_flag and not skipMissing:
            print " Dataset doesn't have uniform number of columns " \
                                         "Line skip is disabled. Parsing failed"
            exit(1)
        elif not all_equal_flag and skipMissing:
            print " Dataset doesn't have uniform number of columns " \
                  "Line skip is disabled. Skipped non-confirming lines"
            parsed_data = filterFrequentlyOccurringColumns(parsed_data, freq_key)

    header_data, parsed_data = formatDataset_InferColumnDataType(parsed_data, header, freq_key, header_data)
    return header_data, parsed_data

# Test Cases

# Fail Test Cases
# File doesn't exist
# print customCSV_Parser(fileName="NonExistentFile.csv", skip=2, header=True)
# Header flag = True & Incorrect number of columns & Skip bad lines disabled
# print customCSV_Parser(fileName="/Users/ashwinkumar/Desktop/ZionsBankCorpChallenge/TestData/Incorrect_Num_Columns_No_Quotes_Character_W_Header.csv", header=True, skipMissing=False)
# Header flag = False & Incorrect number of columns & Skip bad lines disabled
# print customCSV_Parser(fileName="/Users/ashwinkumar/Desktop/ZionsBankCorpChallenge/TestData/Incorrect_Num_Columns_No_Quotes_Character_WO_Header.csv", header=False, skipMissing=False)

# Pass Test Cases
# Header flag = True & Incorrect number of columns & Skip bad lines enabled
# print customCSV_Parser(fileName="/Users/ashwinkumar/Desktop/ZionsBankCorpChallenge/TestData/Incorrect_Num_Columns_No_Quotes_Character_W_Header.csv", header=True, skipMissing=True)
# Header flag = False & Incorrect number of columns & Skip bad lines enabled
print customCSV_Parser(fileName="/Users/ashwinkumar/Desktop/ZionsBankCorpChallenge/TestData/Incorrect_Num_Columns_No_Quotes_Character_WO_Header.csv", header=False, skipMissing=True)

