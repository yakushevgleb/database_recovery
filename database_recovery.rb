require 'json'
require 'fileutils'
# puts "Enter your backup sql file path: "
# backup_path = gets.chomp
# puts "Enter your current sql file path: "
# current_db_path = gets.chomp

class SqlToJson

  @@hashed_file = []

  def parse_file path, table_name
    file = IO.foreach(path).to_a
    start_create_structure = false
    start_write_data = false
    table_structure = {}
    hashed_file = []
    written_rows = 0
    file.each do |line|
      line = line.scrub
      if line.match(/CREATE TABLE [`]#{table_name}[`] [(]/)
        start_create_structure = true
        next
      end
      if line.match(/PRIMARY KEY/)
        start_create_structure = false
      end
      if start_create_structure && line.match(/\s[`].*[`]\s/)
        table_structure[line[/[`].*[`]/].gsub(/[`]/, '').to_sym] = nil
      end
      if line.match(/INSERT INTO [`]#{table_name}[`] VALUES/)
        start_write_data = true
      else
        start_write_data = false
      end
      if start_write_data
        records_list = line.gsub(/INSERT INTO [`]#{table_name}[`] VALUES [(]|[)];\Z/, '').chomp.split('),(')
        written_rows = written_rows + 1
        record_size = records_list.size
        records_list.each_with_index do |record, index|
          console_status("Get #{index + 1} of #{table_name} record")
          values_list = separate_values(record)
          hashed_file << structure_with_values(table_structure, values_list)
        end
        console_separator
      end
      if written_rows > 0 && start_write_data == false
        break
      end
    end
    @@hashed_file = hashed_file
    hashed_file
  end

  def write_file path
    file_json = File.new(path, "w")
    file_json.puts(@@hashed_file.to_json)
    file_json.close
    file_json
  end

  private

  def structure_with_values structure, values_arr
    filled_structure = structure.clone
    filled_structure.each_with_index do |(key, value), index|
      filled_structure[key] = values_arr[index]
    end
    filled_structure
  end

  def separate_values row
    value = ""
    quote_opened = false
    prev_symb = ''
    result_arr = []
    row.split("").each do |char|
      case char
      when "'"
        quote_opened = !quote_opened
        if prev_symb == '\\'
          quote_opened = true
        else
          next
        end
      when ","
        if !quote_opened
          result_arr << value
          value = ""
          next
        end
      end
      prev_symb = char
      value << char
    end
    result_arr
  end

  def console_status status
    printf("\r#{status}")
  end

  def console_separator
    printf("\n---------------------------\n")
  end
end

p "==================Parsing backup=================="

backup_parser = SqlToJson.new
backup_parser.parse_file("/Users/gleb/sophie_backup_2017_08_31.sql", 'blocks')
backup_parser.write_file("./tmp/backup.json")

p "==================Parsing current DB file=================="

current_db_parser = SqlToJson.new
backup_parser.parse_file("/Users/gleb/sophie_backup_2017_08_31.sql", 'blocks')
backup_parser.write_file("./tmp/current_db.json")

# backup_json = File.new("./tmp/backup.json", "w")
# backup_json.puts(hashed_backup.to_json)
# backup_json.close
