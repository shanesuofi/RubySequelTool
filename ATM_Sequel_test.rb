require 'rubygems'
require 'sequel'
require 'couchrest'

# Classes --------------------------------------------------------------------
class AccessDb
  attr_accessor :mdb, :connection, :dataset, :data, :fields
  attr_reader :fieldCount	#new

  def initialize(mdb=nil)
    @mdb = mdb
    @database = nil
    @dataset = nil
    @data = nil
    @fields = nil
    @fieldCount = nil	#new
  end

  # Makes a connectoin to an Access database.
  def open
    connection_string =  'Provider=Microsoft.Jet.OLEDB.4.0;Data Source='
    connection_string << @mdb
    @database = Sequel.ado(:conn_string=>connection_string) # connect to Access DB
  end

  # Takes an sql query string, runs it, and stores the results.
  def query(sql)
    @dataset = @database[sql]   #Take SQL string and connect it to a DB
    #puts("Datset", dataset)
    @fields = Array.new         #Create blank array of field/column names
    @fields = @dataset.columns  #Gets table field/column names
    #print("Fields", @fields, "\n") #debug  
    @fieldCount = @fields.length

    begin
      @data = @dataset.all  #Executes SQL
      rescue
        @data = Array.new
    end
  end

  def execute(sql)
    @database[sql].all
  end

  def close
    @database = nil
  end
end

#-----------------------------------------------------------------------------
# Helper functions

#Displays the field (columne) names or records (rows) of the Access DB query
# dependign on the given flag.
#Flags: f = show fields, r = show records.
def showTable(db, flag)
  fieldNames = db.fields 
  rows = db.data
  
  case(flag)
    when "f" then puts("*** Field Names #{fieldNames.length} *** \n", fieldNames, "\n")
    when "r" then puts("*** Records *** \n", rows)
    when "p" then print("*** Records *** \n", rows) #debug
  end
end
#-----------------------------------------------------------------------------
# Write results of Access DB query to a file.
def writeTable(outf, db)
  fieldNames = db.fields
  rows = db.data
  
  #outf.puts("*** Field Names (#{fieldNames.length}) *** \n", fieldNames, "\n") # debug
  #outf.puts("*** Records *** \n", rows) # debug
  outf.puts(rows)
end
#-----------------------------------------------------------------------------
# Creates an SQL query to get all the records from a table.
def makeQueryTableAll(tableName)
  #sql = "SELECT * FROM [" << tableName << "]"
  sql = "SELECT * FROM [" << tableName << "]" << " WHERE id < 3"
  #puts(sql) #debug
  return sql
end
#-----------------------------------------------------------------------------
#Converts a table from SQL to a hash for easy entry into CouchDB. 
def tableConvert(tableName, table)
  dbTableH = Hash.new
  records = Array.new
  i = 0
  
  table.each do
    |row|
    records[i] = row
    i += 1
  end
  dbTableH[tableName] = records
  
  return dbTableH
end
#-----------------------------------------------------------------------------

# Connect to MS Access DB
inf = File.open("TableListSmall.txt", "r")
dbPath = inf.readline
puts("Opening file...\n", dbPath) #console feedback
metdb = AccessDb.new(dbPath)
metdb.open

# Query Access DB
tableLines = inf.readlines  #read querys from file
tableLines.each {|str| str.delete!("\n")}  #remove newline characters
#print(tblLines, "\n") # debug
#tblLines2 = tblLines.delete("\n") #remove newline characters
#puts(tblLines) # debug
#print(tblLines2, "\n") # debug

#outf = File.new("metTestOut.txt", "w")

# Create CouchDB
metCouch = CouchRest.database!('methow-db')

tableLines.each{|tableName|
  #puts("$ Processing... ", tblName) #console feedback
  sql = makeQueryTableAll(tableName)
  puts("$ Processing... ", sql) #console feedback
  metdb.query(sql)              #Get SQL query
  #print(sql) #debug
  #fieldNames = metdb.fields    #Get SQL fields (just the fields)
  #puts("Field Names", fieldNames) # debug
  table = metdb.data            #Get the Access DB table
  #puts("Rows", rows) # debug
  dbTable = tableConvert(tableName, table)  #Convert to hash for entry into CouchDB
  puts("*** dbTable ***", dbTable) #debug
  response = metCouch.save_doc(dbTable)
  doc = metCouch.get(response['id'])
  puts("doc", doc)
  #writeTable(outf, metdb)
}
puts("$ Finished processing.")

inf.close
#outf.close
metdb.close