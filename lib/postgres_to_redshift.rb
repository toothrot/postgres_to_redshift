require "helper/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'slack-notifier'
require 'zlib'
require 'tempfile'
require "helper/table"
require "helper/column"
require "helper/slack_notifier"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :source_schema, :source_table, :target_uri, :target_schema, :delete_option, :condition_field, :condition_value
  end

  attr_reader :source_connection, :target_connection, :s3, :table

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.update_tables
    update_tables = PostgresToRedshift.new
    if update_tables.tables.size == 0
      message = "[P2RS]MISSING: Table(s) not found using the following parameters:\n[P2RS]MISSING: source_schema: #{ENV["P2RS_SOURCE_SCHEMA"]}\n[P2RS]MISSING: source_table: #{ENV["P2RS_SOURCE_TABLE"]}\n[P2RS]MISSING: delete_option: #{ENV["P2RS_DELETE_OPTION"]}"
      SLACK_NOTIFIER.ping message
    end
    update_tables.tables.each do |table|
      update_tables.copy_table(table)
      update_tables.import_table(table)
    end
    if (PostgresToRedshift.delete_option != 'incremental')
      message = "[P2RS]SUCCESS: Table(s) #{PostgresToRedshift.delete_option} and copy to RedShift | SCHEMA: #{PostgresToRedshift.target_schema}"
      SLACK_NOTIFIER.ping message
    end
  rescue => e
    SLACK_NOTIFIER.ping "[P2RS]#{e.message.gsub("\r"," ").gsub("\n"," ")}| SCHEMA: #{PostgresToRedshift.target_schema} | TABLE: #{update_tables.table.target_table_name}  | OPTION: #{PostgresToRedshift.delete_option}"
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['P2RS_SOURCE_URI'])
  end

  def self.source_schema
    @source_schema ||= ENV['P2RS_SOURCE_SCHEMA']
  end

  def self.source_table
    @source_table ||= ENV['P2RS_SOURCE_TABLE']
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['P2RS_TARGET_URI'])
  end

  def self.target_schema
    @target_schema ||= ENV['P2RS_TARGET_SCHEMA']
  end

  def self.delete_option
    @delete_option ||= ENV["P2RS_DELETE_OPTION"]
  end

  def self.condition_field
    @condition_field ||= ENV["P2RS_CONDITION_FIELD"]
  end

  def self.condition_value
    @condition_value ||= ENV["P2RS_CONDITION_VALUE"]
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def tables
    if (PostgresToRedshift.source_table == 'ALL' && PostgresToRedshift.delete_option != 'incremental')
      table_command = <<-SQL
        SELECT t.*
        FROM information_schema.tables t
        WHERE t.table_schema = '#{PostgresToRedshift.source_schema}' AND t.table_type in ('BASE TABLE') AND t.table_name NOT IN ('ar_internal_metadata','schema_migrations') AND LEFT(t.table_name,1) != '_'
        ORDER BY t.table_name
      SQL
    elsif (PostgresToRedshift.source_table == 'ALL' && PostgresToRedshift.delete_option == 'incremental')
      table_command = <<-SQL
        SELECT t.*
        FROM information_schema.tables t
          INNER JOIN information_schema.columns c1 ON t.table_name = c1.table_name AND t.table_schema = c1.table_schema AND c1.column_name = 'id'
          INNER JOIN information_schema.columns c2 ON t.table_name = c2.table_name AND t.table_schema = c2.table_schema AND c2.column_name = '#{PostgresToRedshift.condition_field}'
        WHERE t.table_schema = '#{PostgresToRedshift.source_schema}' AND t.table_type in ('BASE TABLE') AND t.table_name NOT IN ('ar_internal_metadata','schema_migrations') AND LEFT(t.table_name,1) != '_'
        ORDER BY t.table_name
      SQL
    elsif (PostgresToRedshift.source_table != 'ALL' && PostgresToRedshift.delete_option != 'incremental')
      table_command = <<-SQL
        SELECT t.*
        FROM information_schema.tables t
        WHERE t.table_schema = '#{PostgresToRedshift.source_schema}' AND t.table_name = '#{PostgresToRedshift.source_table}'
        ORDER BY t.table_name
      SQL
    elsif (PostgresToRedshift.source_table != 'ALL' && PostgresToRedshift.delete_option == 'incremental')
      table_command = <<-SQL
        SELECT t.*
        FROM information_schema.tables t
          INNER JOIN information_schema.columns c1 ON t.table_name = c1.table_name AND t.table_schema = c1.table_schema AND c1.column_name = 'id'
          INNER JOIN information_schema.columns c2 ON t.table_name = c2.table_name AND t.table_schema = c2.table_schema AND c2.column_name = '#{PostgresToRedshift.condition_field}'
        WHERE t.table_schema = '#{PostgresToRedshift.source_schema}' AND t.table_name = '#{PostgresToRedshift.source_table}'
        ORDER BY t.table_name
      SQL
    else
      puts "ERROR: variables not consistent with application specification"
    end
    source_connection.exec(table_command).map do |table_attributes|
    @table = Helper::Table.new(attributes: table_attributes)
    next if table.name =~ /^pg_/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    column_command = <<-SQL
      SELECT *
      FROM information_schema.columns
      WHERE table_schema = '#{PostgresToRedshift.source_schema}' AND table_name='#{table.name}' AND LOWER(column_name) NOT LIKE '%password%' AND LOWER(column_name) NOT LIKE '%token%' AND lower(column_name) NOT LIKE '%encrypted%'
      ORDER BY ordinal_position
    SQL
    source_connection.exec(column_command)
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['P2RS_S3_EXPORT_ID'], secret_access_key: ENV['P2RS_S3_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['P2RS_S3_EXPORT_BUCKET']]
  end

  def copy_table(table)
    tmpfile = Tempfile.new("psql2rs")
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 5 * GIGABYTE # uncompressed
    chunk = 1

    bucket.objects.with_prefix("#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz").delete_all

    begin
      puts "DOWNLOADING #{table}"
      if PostgresToRedshift.delete_option != 'incremental'
        copy_to_command = <<-SQL
          COPY (
            SELECT #{table.columns_for_copy}
            FROM #{PostgresToRedshift.source_schema}.#{table.name}
            ORDER BY id
            ) TO STDOUT WITH DELIMITER '|'
        SQL
      elsif PostgresToRedshift.delete_option == 'incremental'
        copy_to_command = <<-SQL
          COPY (
            SELECT #{table.columns_for_copy}
            FROM #{PostgresToRedshift.source_schema}.#{table.name}
            WHERE #{PostgresToRedshift.condition_field} > (localtimestamp - interval '#{PostgresToRedshift.condition_value} minute')
            ORDER BY id
            ) TO STDOUT WITH DELIMITER '|'
        SQL
      else
        puts "ERROR: variables not consistent with application specification"
      end
      source_connection.copy_data(copy_to_command) do
        while row = source_connection.get_copy_data
          zip.write(row)
          if (zip.pos > chunksize)
            zip.finish
            tmpfile.rewind
            upload_table(table, tmpfile, chunk)
            chunk += 1
            zip.close unless zip.closed?
            tmpfile.unlink
            tmpfile = Tempfile.new("psql2rs")
            zip = Zlib::GzipWriter.new(tmpfile)
          end
        end
      end
      zip.finish
      tmpfile.rewind
      upload_table(table, tmpfile, chunk)
      source_connection.reset
    ensure
      zip.close unless zip.closed?
      tmpfile.unlink
    end
  end

  def upload_table(table, buffer, chunk)
    puts "UPLOADING #{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz.#{chunk}"

    bucket.objects["#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz.#{chunk}"].write(buffer, acl: :authenticated_read)

  end

  def import_table(table)
    puts "IMPORTING #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
    if (PostgresToRedshift.delete_option == 'drop' || PostgresToRedshift.delete_option == 'truncate')
      copy_from_command = <<-SQL
        COPY #{PostgresToRedshift.target_schema}.#{table.target_table_name}
        FROM 's3://#{ENV['P2RS_S3_EXPORT_BUCKET']}/#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz'
        CREDENTIALS 'aws_access_key_id=#{ENV['P2RS_S3_EXPORT_ID']};aws_secret_access_key=#{ENV['P2RS_S3_EXPORT_KEY']}'
        GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|' COMPUPDATE ON
      SQL
      if PostgresToRedshift.delete_option == 'drop'
        puts "DROP TABLE IF EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
        target_connection.exec("DROP TABLE IF EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}")
        puts "CREATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
        target_connection.exec("CREATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name} (#{table.columns_for_create})")
        puts "COPY TABLE to #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
        target_connection.exec(copy_from_command)
      elsif PostgresToRedshift.delete_option == 'truncate'
        puts "CREATE TABLE IF NOT EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
        target_connection.exec("CREATE TABLE IF NOT EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name} (#{table.columns_for_create})")
        puts "TRUNCATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
        target_connection.exec("TRUNCATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}")
        puts "COPY TABLE to #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
        target_connection.exec(copy_from_command)
      else
        puts "ERROR: variables not consistent with application specification"
      end
    elsif PostgresToRedshift.delete_option == 'incremental'
      copy_from_command = <<-SQL
        COPY #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp
        FROM 's3://#{ENV['P2RS_S3_EXPORT_BUCKET']}/#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz'
        CREDENTIALS 'aws_access_key_id=#{ENV['P2RS_S3_EXPORT_ID']};aws_secret_access_key=#{ENV['P2RS_S3_EXPORT_KEY']}'
        GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|' COMPUPDATE ON
      SQL
      puts "DROP TABLE IF EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp"
      target_connection.exec("DROP TABLE IF EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp")
      puts "CREATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp"
      target_connection.exec("CREATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp (#{table.columns_for_create})")
      puts "COPY TABLE to #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp"
      target_connection.exec(copy_from_command)
      puts "DELETE FROM #{PostgresToRedshift.target_schema}.#{table.target_table_name} USING #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp T WHERE #{PostgresToRedshift.target_schema}.#{table.target_table_name}.id = T.id"
      target_connection.exec("DELETE FROM #{PostgresToRedshift.target_schema}.#{table.target_table_name} USING #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp T WHERE #{PostgresToRedshift.target_schema}.#{table.target_table_name}.id = T.id")
      puts "INSERT INTO #{PostgresToRedshift.target_schema}.#{table.target_table_name} SELECT * FROM #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp"
      target_connection.exec("INSERT INTO #{PostgresToRedshift.target_schema}.#{table.target_table_name} SELECT * FROM #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp")
      puts "DROP TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp"
      target_connection.exec("DROP TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}_temp")
      #puts "VACUUM #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      #target_connection.exec("VACUUM #{PostgresToRedshift.target_schema}.#{table.target_table_name}")
      #puts "ANALYZE #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      #target_connection.exec("ANALYZE #{PostgresToRedshift.target_schema}.#{table.target_table_name}")
    else
      puts "ERROR: variables not consistent with application specification"
    end
  end
end
