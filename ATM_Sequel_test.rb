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
    @fields = []                #Create blank array of field/column names
    @fields = @dataset.columns  #Gets table field/column names
    #print("Fields", @fields, "\n") #debug  
    @fieldCount = @fields.length
    
    @data = @dataset.all  #Executes SQL

    begin
      @data = @dataset.all  #Executes SQL
      rescue
        @data = []
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

# Write results of Access DB query to a file.
def writeTable(outf, db)
  fieldNames = db.fields
  rows = db.data
  
  #outf.puts("*** Field Names (#{fieldNames.length}) *** \n", fieldNames, "\n") # debug
  #outf.puts("*** Records *** \n", rows) # debug
  outf.puts(rows)
end

#Converts a table from SQL to a hash for entry into CouchDB. 
def tableConvert(fieldNames, rows)
  dbTableH = Hash.new
  i = 0
  
  fieldNames.each do
    |field|
    dbTableH[field] = rows.map(fieldNames[i])
    i = i + 1
  end
  
  return dbTableH
end
#-----------------------------------------------------------------------------

# Connect to MS Access DB
inf = File.open("SQLlist.txt", "r")
dbPath = inf.readline
puts("Opening file...\n", dbPath) #console feedback
metdb = AccessDb.new(dbPath)
metdb.open

# Query Access DB
sqlLines = inf.readlines  #read querys from file
sqlLines.delete("\n")     #remove newline characters

#outf = File.new("metTestOut.txt", "w")

# Create CouchDB
#metCouch = CouchRest.database!('methow-db')

sqlLines.each{|sql|
  puts("$ Processing... ", sql) #console feedback
  metdb.query(sql)            #Get SQL query
  #print(sql) #debug
  fieldNames = metdb.fields   #Get SQL fields (just the fields)
  #puts("Field Names", fieldNames)  # debug
  rows = metdb.dataset           #Get the Access DB rows (all the rows, no fields)
  #puts("Rows", rows) # debug
  dbTable = tableConvert(fieldNames, rows)  #Convert to hash for entry into CouchDB
  puts("*** dbTable ***", dbTable) #debug
  #response = metCouch.save_doc(dbTable)
  #writeTable(outf, metdb)
}

inf.close
#outf.close
metdb.close