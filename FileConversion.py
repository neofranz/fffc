
# coding: utf-8

# ## Test by Jose-Francisco Zubizarreta
# ### Text document into formatted csv based on metadata

# In[3]:

#Import the relevant libraries
import pandas as pd
import numpy as np
import datetime
import itertools



def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1




def get_blocks_to_read(txt_path):
    total_rows = file_len(txt_path)#%100000
    with open(txt_path, 'r') as f:
        bytes_per_row = len(f.readline()) #assuming same row size accross the file and 4 bytes per row
    file_size_approx = total_rows*bytes_per_row # 138 bytes for SampleFile.txt, very accurate wrt to real size of file
    blocks_to_read = (file_size_approx/100000000)+1 # Divide file size in bytes by 100 MBytes, data to load each time
    rows_to_read   = 100000000/bytes_per_row
    return blocks_to_read, rows_to_read



def pre_process_part(part, particles, idx):

    if  particles.loc[idx,'datatype']=='date': #If substring has 'date' data type extract the date component
        #try:
        part2= datetime.datetime.strptime(part, '%Y-%m-%d')    
        part = datetime.date.strftime(part2, "%d/%m/%y")
    else:
        part = part.strip() #removes leading and trailing spaces to only have relevant txt in csv file
    if particles.loc[idx,'datatype']=='string':
        part = '"' + part + '"'
    return part


blocks, rows_to_read = get_blocks_to_read(txt_path)
print rows_to_read


def format_row_to_append(particles, line):
    
    #Use sub indexes to break the text lines into different particles (one per each data varible and data type)   
                
    sub_idx = 0
    row_to_append = []
    for idx in range(0,len(particles)):          
        current_length = particles.loc[idx,'length'] #Get length of the substring to get

        sub_end = sub_idx + current_length
        part = line[sub_idx:sub_end]                   

        part = pre_process_part(part, particles,idx)

        row_to_append.append(part) # append data to new row
        sub_idx = sub_end
        
    return row_to_append


def transform_file(txt_path, particles):

    blocks_to_read, rows_to_read = get_blocks_to_read(txt_path)

    rows_to_read = 3 #rows_to_read
    with open (txt_path, "r") as myfile:
        for block_idx in range(blocks_to_read):

            #data = myfile.readlines() #Open text file 
            #for line_idx, line in enumerate(data): # read line by line
            line_idx = 0
            for line in itertools.islice(myfile, 0, rows_to_read):
                #print line

                if line_idx == 0:
                    transformed =  pd.DataFrame(columns = particles.loc[:,'var_name'].values)

                row_to_append = format_row_to_append(particles, line)
                
                #transformed.append(row_to_append)
                transformed.loc[line_idx,:]=row_to_append #add new row
                line_idx += 1

            print transformed.sort_index()
            block_idx += 1
            #Save transformed data into csv format
            transformed.to_csv('/media/zubizarreta/Dt2/Data/TextProcessing/Octo/New_data.csv', index = False)





#Main code

txt_path      = '/media/zubizarreta/Dt2/Data/TextProcessing/Octo/SampleFile.txt'

if txt_path.endswith('.txt'):

    metadata_path = '/media/zubizarreta/Dt2/Data/TextProcessing/Octo/Metadata.txt'

    #By particles I refer to a dataframe loaded with the metadata
    particles = pd.read_csv(metadata_path, names = ['var_name','length','datatype'])
    
    transform_file(txt_path, particles)
else: 
    print 'Wrong file format'





