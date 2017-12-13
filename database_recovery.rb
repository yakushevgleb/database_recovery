require 'json'
require 'fileutils'

class SqlToJson

  @@hashed_file = {}

  def parse_file path, table_name
    file = IO.foreach(path).to_a
    start_create_structure = false
    start_write_data = false
    table_structure = {}
    hashed_file = {}
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
          full_record = structure_with_values(table_structure, values_list)
          hashed_file[full_record[:id]] = full_record
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


# ---------Index Code----------- "/Users/gleb/sophie_backup_2017_08_31.sql" "/Users/gleb/MySQL.sql"

p "Enter your backup sql file path: "
backup_path = gets.chomp
p "Enter your current sql file path: "
current_db_path = gets.chomp
p "Enter your current DB name: "
current_db_name = gets.chomp

table_name = 'blocks';
field_to_recover = 'owner_id'

result_dir = "result"

unless File.directory?(result_dir)
  Dir.mkdir result_dir
end


p "==================Parsing backup=================="

backup_parser = SqlToJson.new
backup_hashes = backup_parser.parse_file(backup_path, table_name)

p "==================Parsing current DB file=================="

current_db_parser = SqlToJson.new
current_hashes = current_db_parser.parse_file(current_db_path, table_name)

p "==================Comparing two results and writing into file=================="


result_path = "./result/result_script.sql"
File.delete(result_path) if File.exist?(result_path)
result_file = File.new(result_path, "w")

result_file.puts "USE `#{current_db_name}`;"

updated_count = 0

backup_hashes.each do |(key, value)|
  if current_hashes.key?(key)
    if value[field_to_recover.to_sym] != "NULL" && current_hashes[key][field_to_recover.to_sym] == "NULL"
      result_file.puts "UPDATE `#{table_name}` SET `#{field_to_recover}`='#{value[field_to_recover.to_sym]}' WHERE `id`=#{value[:id]};"
      updated_count = updated_count + 1
    end
  end
end

result_file.close

p "File's created! #{updated_count} records of #{table_name} will be updated"
