# importing libraries
import csv
import os.path
import logging
from typing import Dict,List, Tuple
import re
from functools import reduce
import traceback

class DB:
    """Database class which implements all the operations of a primitive database."""
    
    # constructor 
    def __init__(self):
        #datafile pointer
        self.data_file_ptr = None
        # config file pointer
        self.config_file_ptr= None
    
        self.num_records = 10
        self.cFlag=True
        self.record={"Name": None,"Rank": None,"City": None,"State": None,"Zip": None,"Employees": None}
    
        #logging.basicConfig(filename="Database.log",encoding='utf-8',level=logging.DEBUG,format="%(asctime)s %(levelname)s: %(message)s")
    
    
    # create a new database
    def createDB(self,filename,field_size):
        # create a file name
        self.csv_filename = filename +".csv"
        self.config_file= filename+ ".config"
        self.data_file= filename+".data"
        
        
        #used for initial creation
        self.overflow = 0
        
        # Read the CSV file and write into data files
        try:
            #write configuration file
            self.num_records=10 
            self.writeConfigFile(field_size,flag=True)
            with open(self.csv_filename, "r") as csv_file:
                csv_reader= csv.reader(csv_file)
                self.data_file_ptr=open(self.data_file,"w+")
                
                for data in csv_reader:
                    #loop throught the data and write at a time
                    self.writeRecord(data)
                #close the database pointer
                self.close()
            
            print(f"[Info] {filename} Database created..")
            #logging.info(f"[Info] {filename} Database created..")
            self.cFlag=False
            return True
        except Exception as e:
            traceback.print_exc()
            return False,e
        
        
    #Function to write the Configurations file
    def writeConfigFile(self,field_size:Tuple,flag):
        """
        Function to write the field size in the Configurations file

        Args:
            field_size (Tuple): Tuple consiting the length of NAME,RANK,CITY,STATE,ZIP,EMPLOYEES
        """        
        if self.cFlag and flag:
            with open(self.config_file,"w+") as config:
                config.seek(0)
                for item in field_size:
                    config.write(str(item)+' ')
                #writing number of records in config file:
                config.write(str(self.num_records)+ " ")
            #print(f"[Info] Configurations file written....")
        
        #writing overFlow variable:
        with open(self.config_file,"r+") as config:
            config.seek(17)
            config.write(str(self.overflow)) 
            
        
        
        
    
    #function to read the Configurations file
    def readConfigFile(self)->List:
        """
        Returns the field size from the configuration File

        Returns:
            List: List consiting the length of NAME,RANK,CITY,STATE,ZIP,EMPLOYEES
        """        
        with open(self.config_file,"r+") as config:
                config.seek(0)
                data= config.read().split(" ")
                return data
                
                
    # function to write a data record to the data file
    def writeRecord(self,data:List):
        """
        Write a record to the current opened data file at the end.

        Args:
            data (Dict): Dictionary of NAME,RANK,CITY,STATE,ZIP,EMPLOYEES

        Returns:
            Bool: returns true if the writing of the data is successfull, eslese false
        """
        
        self.data_sizes=self.readConfigFile()
        if len(data)!=6:
            self.data_sizes=self.data_sizes[1:6]
        else:
            self.data_sizes=self.data_sizes[:6]
        try:
            for item, data_size in zip(data,self.data_sizes):
                self.data_file_ptr.write(item.ljust(int(data_size)))
            if len(data)==6:   
                self.data_file_ptr.write("\n")
        
        except Exception as e:
            print("[Error] Could not write",e)
        

                                 
                  
   # function to open the database     
    def open(self,filename: str):
        """
        Opens the config file to read numRecords, opens the data file in read/write mode and sets the dataFileptr to open the file, updates values in any another intance variables

        Args:
            filename (str): Name if the databse to open
        
        returns:
        Boolean: true if the open was successful
        """        
        try:
            if self.data_file_ptr==None:
                self.config_file=filename+".config"
                self.data_file=filename+".data"
                
                #config_file pointer to open configuration file
                self.config_file_ptr=open(self.config_file,"r+")
                self.data_file_ptr=open(self.data_file,"r+")
                
                self.num_records=10
                
                print(f"[INFO] {filename} Database successfully opened.")
                #logging.info(f"{filename} Database successfully opened.")
                return True
            else:
                print(f"[ERROR] Another database is already opened. Please close the current database to open a new one.")
        
        except Exception as e:
            print(f"[ERROR] {filename} database not found. Please create a new one")
            return False
    
    # function to close the database 
    def close(self):
        """
        resets instance variables, e.g., sets numRecords to 0, dataFileptr to NULL, close the file
        
        """  
        try:
            #close the database
            self.file_name=self.data_file_ptr.name.split(".")[0]
            self.data_file_ptr.close() 
            self.data_file_ptr = None
            self.num_records=0
            
            # print(f"[INFO] {file_name} closed successfully")
            # logging.info(f"{file_name} closed successfully.")
            return True
        except Exception as e:
            #print("[Error] Database closed failed, Please open Database to close the database.",e )
            return False
        
    #function to check if the database is open or not
    def isOpen(self)-> bool:
        """
        allow main program to check the status of the DB.
        returns: Boolean (true if database is open, false if not)
        """        

        if self.data_file_ptr is not None:
            file_name=self.data_file_ptr.name.split(".")[0]
            #print(f"{file_name} is currently opened.")
            return  True
        else:
            #print("[Error] No database are opened")
            return False
    
    #read the database
    def readDB(self, DBsize, rec_size):
        self.DB_size = DBsize+int(self.readConfigFile()[-1])
        self.rec_size = rec_size
        if self.isOpen():
            return True
        else:
            print(f"[Error] Database is not open. Please open the database")

    # function to read a record from the database
    def readRecord(self,recordNum):
        flag=False
        Name=Rank=City=State=Zip=Employees=None
        
        try:
            if recordNum >=0 and recordNum <self.DB_size:
                # move pointer to initial location
                self.data_file_ptr.seek(0)
                # move pointer to the record to read
                
                self.data_file_ptr.seek(recordNum*(self.rec_size+2))
                line=self.data_file_ptr.readline()
                flag=True
            else:
                print("[Error] Record number out of range")
            if flag:
                pattern=re.compile(r'(.{50})(.{4})(.{20})(.{3})(.{6})(.{8})')
                matches=pattern.findall(line)
                #print(matches)
                Name,Rank,City,State,Zip,Employees= [x.rstrip() for x in list(matches[0])]
            #set record
            self.record = dict({"Name":Name,"Rank":Rank,"City":City,"State":State,"Zip":Zip, "Employees": Employees})
            
                
        except Exception as e:
            #print("[Error] Record number out of range",e)
            pass
    
    #Binary Search by record id
    def binarySearch (self, name):
        low = 0
        high = self.num_records - 1
        self.found_loc=-1
        while high >= low:

            self.middle = (low+high)//2
            self.readRecord(self.middle)
            # print(self.record)
            mid_id = self.record["Name"]
            
            if mid_id == name:
                self.found_loc=self.middle
                return True
            elif mid_id > name:
                high = self.middle - 1
            elif mid_id < name:
                low = self.middle + 1
    
    # function to check if the data is in the file
    def findRecord(self,input_ID: str):
        """
        Updates the self.record and the self.found_loc variable.

        Args:
            input_ID (str): name of the company

        Returns:
            _type_: _description_
        """        
        # check for the binary search, if data found return pointer
        if self.binarySearch(input_ID)==True and self.isOpen():
            return True
        
        elif self.linearSearch(input_ID)==True and self.isOpen():
            return True
        else:
            #print(f"[Error] Data not found")
            return False
    
    # Function to update the data in the database if the primary key matches
    def updateRecord(self,data:List,flag):
        """
        if db is open, it uses findRecord to locate the record. It then uses writeRecord to overwrite it. NOTE: it assumes that the key (name) will not be changed or binarySearch will break.

        Args:
            data (dict): dictionary containing name, rank, city, state, zip, employees
        Returns:
            Boolean, true if record is updated, false otherwise
        """
        if  flag:
            #finding the location to write the new content
            location=self.found_loc*(self.rec_size+2) + int(self.readConfigFile()[0])
            # print(location)
            
            self.data_file_ptr.close()
            self.data_file_ptr=open(self.data_file,"r+")
            self.data_file_ptr.seek(0)
            self.data_file_ptr.seek(location)
            # writing data except the name
            self.writeRecord(data[1:])
            #print(f"[INFO] Database updated for {primary_key}")
            self.data_file_ptr.close()
            self.data_file_ptr=open(self.data_file,"r+")
            return True

            
        else:
            return False
            
    
    #funcition to delete the record in the file if the primary key matches
    def deleteRecord(self,name):
        """
       if db is open, it uses findRecord to locate the record. It then uses writeRecord to overwrite it with default (empty) values. Keeps the name the same though, otherwise binarySearch will break.

        Args:
            name(str): primary key for the database to find the record and delete it
        Returns:
            Boolean, true if record is deleted, false otherwise
        """
        # name as the primary key      
        
        if self.findRecord(name)==True and self.isOpen():
            #finding the location to write the delete content
            location=self.found_loc*(self.rec_size+2) #+ int(self.readConfigFile()[0])
            # print(location)
            
            self.data_file_ptr.close()
            self.data_file_ptr=open(self.data_file,"r+")
            self.data_file_ptr.seek(0)
            self.data_file_ptr.seek(location)
            # writing empty data to the location
            self.writeRecord([" "," "," "," "," "," "])
            self.data_file_ptr.close()
            self.data_file_ptr=open(self.data_file,"r+")
            return True
    
    
    #funcition to add the record in the file and modify the overflow record
    
    def addRecord(self,data:List):
        """
       if db is open, it appending a new fixed length record to the end of the data file.

        Args:
            data (tuple): List containing name, rank, city, state, zip, employees
        Returns:
            Boolean, true if record is deleted, false otherwise
        """
        # name as the primary key      
        
        if self.isOpen():
            #finding the location to write the delete content
            self.data_file_ptr.seek(0,2)
            # # writing data to the end location
            self.writeRecord(data)
            #reading the overflow record numbers
            self.overflow=int(self.readConfigFile()[-1])
            self.overflow+=1
            self.writeConfigFile(None,flag=False)
            self.DB_size = self.DB_size+int(self.readConfigFile()[-1])
            self.data_file_ptr.close()
            self.data_file_ptr=open(self.data_file,"r+")
            return True
        else:
            return False
        
    def linearSearch(self,name):
        """
        Performs linear search on the overflow record. Updates the self.found_loc

        Args:
            name (str): Name of the company to find the data in the overflow
        
        Return
        :True is found else false
        
        """
        #start from the last of the data
        location= self.num_records
        end_location=int(self.readConfigFile()[-1])+location
        #print(location,end_location)
    
        #linear search algorithm
        for i in range(location,end_location):
            self.readRecord(i)
            data=self.record["Name"]
            if name==data:
                self.found_loc=i
                #print("Data found in at",self.found_loc)
                return True
        
        else:
            False
    # Function to create report
    def createReport(self):
        data_sizes=self.readConfigFile()[:6]

            
        data = [next(self.data_file_ptr) for x in range(10)]
        
        for name, lenth in zip(self.record.keys(), data_sizes):
            print(name.center(int(lenth)-2), end = ' ')
        print('')
        for lines in data:
            print(lines.strip())
            
            
            
                

    
        
        
        
            
        
                  
            
        
            
            